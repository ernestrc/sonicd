package build.unstable.sonicd.service.source

import java.sql.{DriverManager, Statement}

import akka.actor.{ActorContext, ActorSystem, Props}
import akka.stream.actor.{ActorPublisher, ActorPublisherMessage}
import akka.testkit.{CallingThreadDispatcher, ImplicitSender, TestActorRef, TestKit}
import build.unstable.sonicd.model.{TypeMetadata, JsonProtocol, OutputChunk, DoneWithQueryExecution}
import build.unstable.sonicd.source.{JdbcConnectionsHandler, JdbcPublisher}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import spray.json._
import JsonProtocol._

class JdbcSourceSpec(_system: ActorSystem)
  extends TestKit(_system) with WordSpecLike
  with Matchers with BeforeAndAfterAll with ImplicitSender
  with ImplicitSubscriber {

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    runQuery(s"DROP ALL OBJECTS")()
    testConnection.close()
  }

  def this() = this(ActorSystem("JdbcSourceSpec"))

  val testDB = "testdb"
  val H2Url = s"jdbc:h2:mem:$testDB;DB_CLOSE_DELAY=-1;"
  val H2Driver = "org.h2.Driver"
  val H2Config =
    s"""
       | {
       |  "driver" : "$H2Driver",
       |  "url" : "$H2Url",
       |  "class" : "JdbcSource"
       | }
    """.stripMargin.parseJson.asJsObject

  val controller: TestActorRef[TestController] =
    TestActorRef(Props[TestController].withDispatcher(CallingThreadDispatcher.Id))

  Class.forName(H2Driver)
  val testConnection = DriverManager.getConnection(H2Url, "SONICD", "")

  def runQuery(q: String)(validation: (Statement) ⇒ Unit = stmt ⇒ ()) = {
    val stmt = testConnection.createStatement()
    stmt.execute(q)
    validation(stmt)
    stmt.close()
  }

  def newPublisher(query: String): TestActorRef[JdbcPublisher] = {
    val src = new JdbcSource(H2Config, "test", query, controller.underlyingActor.context)
    val ref = TestActorRef[JdbcPublisher](src.handlerProps.withDispatcher(CallingThreadDispatcher.Id))
    ActorPublisher(ref).subscribe(subs)
    watch(ref)
    ref
  }

  def expectTypeMetadata() = {
    expectMsgAnyClassOf(classOf[TypeMetadata])
  }

  def expectDone(implicit pub: TestActorRef[JdbcPublisher]) = {
    expectMsg(DoneWithQueryExecution(success = true, Vector.empty))
    expectMsg("complete")
    expectTerminated(pub)
  }

  "JdbcSource" should {
    "run a simple statement" in {
      implicit val pub = newPublisher("create table users(id VARCHAR);")
      pub ! ActorPublisherMessage.Request(1)
      expectTypeMetadata()
      pub ! ActorPublisherMessage.Request(1)
      expectDone
      runQuery("show tables") { stmt ⇒
        val rs = stmt.getResultSet
        rs.next()
        rs.getString(1).toUpperCase shouldBe "USERS"
      }
    }

    "close connection after running one statement" in {
      runQuery("CREATE TABLE test2(id VARCHAR)")()
      implicit val pub = newPublisher("INSERT INTO test2 (id) VALUES ('X')")
      val handle = pub.underlyingActor.handle
      pub ! ActorPublisherMessage.Request(1)
      expectTypeMetadata()
      pub ! ActorPublisherMessage.Request(1)
      expectDone

      runQuery("select count(*) from information_schema.sessions;") { stmt ⇒
        val rs = stmt.getResultSet
        rs.next()
        rs.getInt(1) shouldBe 1 //test connection
      }
    }

    "run a query" in {
      runQuery("CREATE TABLE test3(id VARCHAR)")()
      runQuery("INSERT INTO test3 (id) VALUES ('1234')")()

      implicit val pub = newPublisher("select id from test3")
      pub ! ActorPublisherMessage.Request(1)
      expectTypeMetadata()
      pub ! ActorPublisherMessage.Request(1)
      expectMsg(OutputChunk(Vector("1234")))
      pub ! ActorPublisherMessage.Request(1)
      expectDone
    }

    "send typed values downstream" in {
      val createUsers =
        """
          | CREATE TABLE `users_test`(
          |  `user_id` int,
          |  `email` varchar,
          |  `country` varchar)
        """.stripMargin
      runQuery(createUsers)()
      runQuery("INSERT INTO users_test (user_id, email, country) VALUES (1, '1@lol.com', 'Cat')")()
      runQuery("INSERT INTO users_test (user_id, email, country) VALUES (2, '2@lol.com', NULL)")()
      runQuery("INSERT INTO users_test (user_id, email, country) VALUES (3, NULL, 'Cat')")()
      runQuery("INSERT INTO users_test (user_id, email, country) VALUES (NULL, '4@lol.com', NULL)")()

      implicit val pub = newPublisher("select user_id, email, country from users_test")
      pub ! ActorPublisherMessage.Request(1)
      expectTypeMetadata()
      pub ! ActorPublisherMessage.Request(1)
      expectMsg(OutputChunk(JsArray(Vector(JsNumber(1), JsString("1@lol.com"), JsString("Cat")))))
      pub ! ActorPublisherMessage.Request(1)
      expectMsg(OutputChunk(JsArray(Vector(JsNumber(2), JsString("2@lol.com"), JsNull))))
      pub ! ActorPublisherMessage.Request(1)
      expectMsg(OutputChunk(JsArray(Vector(JsNumber(3), JsNull, JsString("Cat")))))
      pub ! ActorPublisherMessage.Request(1)
      expectMsg(OutputChunk(JsArray(Vector(JsNull, JsString("4@lol.com"), JsNull))))
      pub ! ActorPublisherMessage.Request(1)
      expectDone
    }

    "encode/decode arrays correctly" in {
      val createUsers = """CREATE TABLE `arrays_test`(`int_id` ARRAY)""".stripMargin
      runQuery(createUsers)()
      runQuery(s"INSERT INTO arrays_test VALUES (1)")()
      runQuery(s"INSERT INTO arrays_test VALUES (NULL)")()

      implicit val pub = newPublisher("select * from arrays_test")
      pub ! ActorPublisherMessage.Request(1)
      expectTypeMetadata()
      pub ! ActorPublisherMessage.Request(1)
      expectMsg(OutputChunk(JsArray(Vector(JsArray(Vector(JsString("1")))))))
      pub ! ActorPublisherMessage.Request(1)
      expectMsg(OutputChunk(JsArray(Vector(JsNull))))
      pub ! ActorPublisherMessage.Request(1)
      expectDone
    }

    "encode/decode numbers correctly" in {
      val createNumbers =
        """
          | CREATE TABLE `numbers_test`(
          |  `int_id` int,
          |  `double_id` double,
          |  `float_id` float,
          |  `long_id` bigint)
        """.stripMargin
      runQuery(createNumbers)()
      runQuery(s"INSERT INTO numbers_test VALUES (1, 1.123456789, 1.123456789, ${Long.MaxValue})")()
      runQuery(s"INSERT INTO numbers_test  VALUES (2, NULL, 2.123456789, ${Long.MaxValue})")()
      runQuery(s"INSERT INTO numbers_test  VALUES (3, 3.123456789, NULL, ${Long.MaxValue})")()
      runQuery(s"INSERT INTO numbers_test  VALUES (NULL, 4.123456789, 4.123456789, NULL)")()

      implicit val pub = newPublisher("select * from numbers_test")
      pub ! ActorPublisherMessage.Request(1)
      expectTypeMetadata()
      pub ! ActorPublisherMessage.Request(1)
      val l: Long = Long.MaxValue
      expectMsg(OutputChunk(JsArray(Vector(JsNumber(1), JsNumber(1.123456789), JsNumber(1.123456789), JsNumber(BigInt.apply(l))))))
      pub ! ActorPublisherMessage.Request(1)
      expectMsg(OutputChunk(JsArray(Vector(JsNumber(2), JsNull, JsNumber(2.123456789), JsNumber(BigInt.apply(l))))))
      pub ! ActorPublisherMessage.Request(1)
      expectMsg(OutputChunk(JsArray(Vector(JsNumber(3), JsNumber(3.123456789), JsNull, JsNumber(BigInt.apply(l))))))
      pub ! ActorPublisherMessage.Request(1)
      expectMsg(OutputChunk(JsArray(Vector(JsNull, JsNumber(4.123456789), JsNumber(4.123456789), JsNull))))
      pub ! ActorPublisherMessage.Request(1)
      expectDone
    }

    "should send type metadata" in {
      runQuery("CREATE TABLE test4(id VARCHAR, a BIGINT)")()
      runQuery("INSERT INTO test4 (id, a) VALUES ('1234', 1234)")()
      implicit val pub = newPublisher("select id, a from test4;")
      pub ! ActorPublisherMessage.Request(1)
      expectMsg(TypeMetadata(Vector(("ID", JsString("")), ("A", JsNumber(0)))))
      pub ! ActorPublisherMessage.Request(1)
      expectMsg(OutputChunk(JsArray(Vector(JsString("1234"), JsNumber(1234)))))
      pub ! ActorPublisherMessage.Request(1)
      expectDone
    }
  }
}

//necessary to override jdbc connections actor dispatcher
class JdbcSource(config: JsObject, queryId: String, query: String, context: ActorContext)
  extends build.unstable.sonicd.source.JdbcSource(config, queryId, query, context) {
  override val jdbcConnectionsProps: Props =
    Props(classOf[JdbcConnectionsHandler]).withDispatcher(CallingThreadDispatcher.Id)
}

