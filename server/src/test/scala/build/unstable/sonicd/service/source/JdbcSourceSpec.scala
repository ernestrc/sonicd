package build.unstable.sonicd.service.source

import java.sql.{Connection, DriverManager, Statement}

import akka.actor.{ActorContext, ActorRef, ActorSystem, Props}
import akka.stream.actor.{ActorPublisher, ActorPublisherMessage}
import akka.testkit.{CallingThreadDispatcher, ImplicitSender, TestActorRef, TestKit}
import build.unstable.sonic._
import build.unstable.sonicd.auth.{ApiKey, ApiUser}
import build.unstable.sonicd.model.JsonProtocol._
import build.unstable.sonicd.model._
import build.unstable.sonicd.service.Fixture
import build.unstable.sonicd.source.{JdbcConnectionsHandler, JdbcExecutor}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import spray.json._

class JdbcSourceSpec(_system: ActorSystem)
  extends TestKit(_system) with WordSpecLike
  with Matchers with BeforeAndAfterAll with ImplicitSender
  with ImplicitSubscriber with HandlerUtils {

  import Fixture._

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    runQuery(s"DROP ALL OBJECTS")()
    testConnection.close()
  }

  def this() = this(ActorSystem("JdbcSourceSpec"))

  val H2Url = s"jdbc:h2:mem:JdbcSourceSpec"
  val H2Config =
    s"""
       | {
       |  "driver" : "$H2Driver",
       |  "url" : "$H2Url",
       |  "class" : "JdbcSource"
       | }
    """.stripMargin.parseJson.asJsObject

  val controller: TestActorRef[TestController] =
    TestActorRef(Props(classOf[TestController], self).withDispatcher(CallingThreadDispatcher.Id))

  Class.forName(H2Driver)
  val testConnection = DriverManager.getConnection(H2Url, "SONICD", "")

  def runQuery(q: String)(validation: (Statement) ⇒ Unit = stmt ⇒ ()) = {
    val stmt = testConnection.createStatement()
    stmt.execute(q)
    validation(stmt)
    if (!testConnection.getAutoCommit) testConnection.commit()
    stmt.close()
  }

  def testConnectionOpen() {
    runQuery("select count(*) from information_schema.sessions;") { stmt ⇒
      val rs = stmt.getResultSet
      rs.next()
      rs.getInt(1) shouldBe 1 //test connections
    }
  }

  def newPublisher(q: String, context: RequestContext = testCtx): ActorRef = {
    val query = new Query(Some(1L), Some("traceId"), None, q, H2Config)
    val src = new JdbcSource(query, controller.underlyingActor.context, context)
    val ref = controller.underlyingActor.context.actorOf(src.handlerProps.withDispatcher(CallingThreadDispatcher.Id))
    ActorPublisher(ref).subscribe(subs)
    watch(ref)
    ref
  }

  "JdbcSource" should {

    "run a simple statement" in {
      val pub = newPublisher("create table users(id VARCHAR);")
      pub ! ActorPublisherMessage.Request(1)
      expectTypeMetadata()
      pub ! ActorPublisherMessage.Request(1)
      expectDone(pub)
      runQuery("show tables") { stmt ⇒
        val rs = stmt.getResultSet
        rs.next()
        rs.getString(1).toUpperCase shouldBe "USERS"
      }
      runQuery("drop table users;")()
      testConnectionOpen()
    }

    //http://www.h2database.com/html/advanced.html
    //Please note that most data definition language (DDL) statements,
    //such as "create table", commit the current transaction. See the Grammar for details.
    "commit changes only if user is authenticated and has write access" in {
      runQuery("CREATE TABLE test_commit(id VARCHAR)")()
      runQuery("INSERT INTO test_commit (id) VALUES ('1234')")()
      //try to delete all data
      val pub = newPublisher("delete from test_commit;",
        RequestContext("a", Some(ApiUser("", 1, ApiKey.Mode.Read, None))))
      pub ! ActorPublisherMessage.Request(1)
      expectTypeMetadata()
      pub ! ActorPublisherMessage.Request(1)
      expectDone(pub)
      runQuery("select count(*) from test_commit") { stmt ⇒
        val rs = stmt.getResultSet
        rs.next()
        rs.getInt(1) shouldBe 1
      }
      runQuery("drop table test_commit;")()
      testConnectionOpen()
    }

    "run multiple statements" in {
      val pub = newPublisher(
        "create table one(id VARCHAR);" +
          "create table two(id VARCHAR);" +
          "--create table two(no VARCHAR);" +
          "create table three(id VARCHAR);" +
          "create table four(id VARCHAR);" +
          "create table five(id VARCHAR); ")
      pub ! ActorPublisherMessage.Request(1)
      expectTypeMetadata()
      pub ! ActorPublisherMessage.Request(1)
      expectDone(pub)
      runQuery("show tables") { stmt ⇒
        var buf = Vector.empty[String]
        val rs = stmt.getResultSet
        rs.next()
        buf :+= rs.getString(1)
        rs.next()
        buf :+= rs.getString(1)
        rs.next()
        buf :+= rs.getString(1)
        rs.next()
        buf :+= rs.getString(1)
        rs.next()
        buf :+= rs.getString(1)
        buf should contain allOf("ONE", "TWO", "THREE", "FOUR", "FIVE")
      }
    }

    "run a query" in {
      runQuery("CREATE TABLE test3(id VARCHAR)")()
      runQuery("INSERT INTO test3 (id) VALUES ('1234')")()

      val pub = newPublisher("-- THIS IS A COMMENT\nselect id from test3")
      pub ! ActorPublisherMessage.Request(1)
      expectTypeMetadata()
      pub ! ActorPublisherMessage.Request(1)
      expectMsg(OutputChunk(Vector("1234")))
      pub ! ActorPublisherMessage.Request(1)
      expectDone(pub)
    }

    "run multiple statements but rollback/not commit if one of them fails" in {
      runQuery("create table roll(id VARCHAR);")()
      val pub = newPublisher(
        "insert into roll VALUES ('1');" +
          "-- COMMENT insert into roll VALUES ('1');" +
          "insert into ROCK VALUES ('2');" +
          "insert into roll VALUES ('2');" +
          "insert into roll VALUES ('4');")

      pub ! ActorPublisherMessage.Request(1)
      expectMsgPF() {
        case d: DoneWithQueryExecution ⇒ assert(d.error.nonEmpty)
      }
      expectMsg("complete")
      expectTerminated(pub)
      runQuery("select count(*) from roll") { stmt ⇒
        val rs = stmt.getResultSet
        rs.next()
        rs.getInt(1) shouldBe 0
      }
    }

    "close connection after running one statement" in {
      runQuery("CREATE TABLE test2(id VARCHAR)")()
      val pub = newPublisher("INSERT INTO test2 (id) VALUES ('X')")
      pub ! ActorPublisherMessage.Request(1)
      expectTypeMetadata()
      pub ! ActorPublisherMessage.Request(1)
      expectDone(pub)

      testConnectionOpen()
    }

    "close connection after running multiple statements" in {
      runQuery("CREATE TABLE girona(id VARCHAR)")()
      val pub = newPublisher(
        "-- THIS IS A COMMENT" +
          "INSERT INTO girona (id) VALUES ('X');" +
          "INSERT INTO girona (id) VALUES ('B');" +
          "INSERT INTO girona (id) VALUES ('C');")
      pub ! ActorPublisherMessage.Request(1)
      expectTypeMetadata()
      pub ! ActorPublisherMessage.Request(1)
      expectDone(pub)

      testConnectionOpen()
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

      val pub = newPublisher("select user_id, email, country from users_test")
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
      expectDone(pub)
      testConnectionOpen()
    }

    "encode/decode arrays correctly" in {
      val createUsers = """CREATE TABLE `arrays_test`(`int_id` ARRAY, `dt` TIMESTAMP)""".stripMargin
      runQuery(createUsers)()
      runQuery(s"INSERT INTO arrays_test VALUES (1, NULL)")()
      runQuery(s"INSERT INTO arrays_test VALUES (NULL, cast('2014-04-28' as timestamp))")()

      val pub = newPublisher("select * from arrays_test")
      pub ! ActorPublisherMessage.Request(1)
      expectTypeMetadata()
      pub ! ActorPublisherMessage.Request(1)
      expectMsg(OutputChunk(JsArray(Vector(JsArray(Vector(JsString("1"))), JsNull))))
      pub ! ActorPublisherMessage.Request(1)
      expectMsg(OutputChunk(JsArray(Vector(JsNull, JsString("2014-04-28 00:00:00.0")))))
      pub ! ActorPublisherMessage.Request(1)
      expectDone(pub)
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

      val pub = newPublisher("select * from numbers_test")
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
      expectDone(pub)
      testConnectionOpen()
    }

    "should send type metadata" in {
      runQuery("CREATE TABLE test4(id VARCHAR, a BIGINT)")()
      runQuery("INSERT INTO test4 (id, a) VALUES ('1234', 1234)")()
      val pub = newPublisher("select id, a from test4;")
      pub ! ActorPublisherMessage.Request(1)
      expectMsg(TypeMetadata(Vector(("ID", JsString("")), ("A", JsNumber(0)))))
      pub ! ActorPublisherMessage.Request(1)
      expectMsg(OutputChunk(JsArray(Vector(JsString("1234"), JsNumber(1234)))))
      pub ! ActorPublisherMessage.Request(1)
      expectDone(pub)
      testConnectionOpen()
    }
  }
}

//override dispatchers
class JdbcSource(query: Query, actorContext: ActorContext, context: RequestContext)
  extends build.unstable.sonicd.source.JdbcSource(query, actorContext, context) {

  override val executorProps: (Connection, Statement) ⇒ Props = { (conn, stmt) ⇒
    Props(classOf[JdbcExecutor], query.query, conn, stmt, initializationStmts, context)
      .withDispatcher(CallingThreadDispatcher.Id)
  }
  override val jdbcConnectionsProps: Props =
    Props(classOf[JdbcConnectionsHandler]).withDispatcher(CallingThreadDispatcher.Id)
}
