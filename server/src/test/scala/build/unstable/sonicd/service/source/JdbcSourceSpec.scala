package build.unstable.sonicd.service.source

import java.sql.{Connection, DriverManager, Statement}

import akka.actor.{ActorContext, ActorRef, ActorSystem, Props}
import akka.stream.actor.{ActorPublisher, ActorPublisherMessage}
import akka.testkit.{CallingThreadDispatcher, ImplicitSender, TestActorRef, TestKit}
import build.unstable.sonic.JsonProtocol._
import build.unstable.sonic._
import build.unstable.sonic.model._
import build.unstable.sonicd.auth.ApiKey
import build.unstable.sonicd.model._
import build.unstable.sonicd.service.Fixture
import build.unstable.sonicd.source.{JdbcConnectionsHandler, JdbcExecutor, JdbcPublisher}
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
    val ref = controller.underlyingActor.context.actorOf(src.publisher.withDispatcher(CallingThreadDispatcher.Id))
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
        RequestContext("a", Some(ApiUser("", 1, AuthConfig.Mode.Read, None))))
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
        case d: StreamCompleted ⇒ assert(d.error.nonEmpty)
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
    def isSelect(query: String): Boolean = JdbcPublisher.IS_SQL_SELECT.pattern.matcher(query).matches

    "identifies select statements correctly" in {
      Queries.select.foreach(q ⇒ isSelect(q) shouldBe true)
      Queries.notSelect.foreach(q ⇒ isSelect(q) shouldBe false)
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

object Queries {
  val select =
    "select count(*) from information_schema.sessions;" ::
      "  select * from utils" ::
      "SELECT distinct country from ( select country,widgets, rank() OVER (partition by sdk, datacenter order by widgets) as rnk from ( select country, partner_id, sdk, datacenter, count(distinct widget_id) Widgets from hello where dt = '${datecut}' and widget_type = 'Controller' group by country, partner_id, sdk , datacenter) sc where widgets > 500) where rnk <= 5" ::
      "select DD.dateday, sum(case when minutes is null then 0 else minutes end) Minutes, sum(Partners) Partners from oops DD left outer join (select dt,count(distinct partner_id) Partners, sum(streamed_subscribed_minutes_video_and_audio+streamed_subscribed_minutes_audio_only+streamed_subscribed_minutes_screen_sharing) Minutes from minutes_hourly us inner join (select distinct browser_name, client_version, sdk, connection_id from connection_dim where dt = '${datecut}')  ccd on ccd.connection_id = us.connection_id left outer join (select account_id, max(vertical) vertical from partnerathon group by account_id) pat on us.account_id = pat.account_id ${DIM_CLAUSE} group by dt) uss on DD.DateDay = uss.dt where DD.DateDay = '${datecut}' group by DD.dateday" ::
      Nil

  val notSelect =
    """ALTER TABLE clientevent_invalid ADD IF NOT EXISTS PARTITION (dt=${DATECUT});
       ALTER TABLE clientevent_logs ADD IF NOT EXISTS PARTITION (dt=${DATECUT});
       ALTER TABLE clientqos_invalid ADD IF NOT EXISTS PARTITION (dt=${DATECUT});
       ALTER TABLE clientqos_logs ADD IF NOT EXISTS PARTITION (dt=${DATECUT});
       ALTER TABLE anvil_logs ADD IF NOT EXISTS PARTITION (dc=${DATECUT});
       ALTER TABLE anvil_error_logs ADD IF NOT EXISTS PARTITION (dc=${DATECUT});
    """ ::
      "CREATE TABLE test4(id VARCHAR, a BIGINT)" ::
      "INSERT INTO numbers_test  VALUES (NULL, 4.123456789, 4.123456789, NULL)" ::
      """SET hive.exec.dynamic.partition.mode=nonstrict;
        |SET hive.exec.dynamic.partition=true;
        |SET hive.exec.compress.output=true;
        |SET hive.exec.compress.intermediate=true;
        |SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
        |SET mapred.reduce.slowstart.completed.maps=0.6;
        |SET hive.mapred.reduce.tasks.speculative.execution=false;
        |SET mapred.reduce.tasks.speculative.execution=false;
        |-- SET hive.exec.reducers.bytes.per.reducer=128000000;
        |set hive.enforce.bucketing = true;
        |SET hive.exec.reducers.max=40;
        |SET mapreduce.reduce.shuffle.input.buffer.percent=0.5;
        |
        |use deduped;
        |
        |DROP TABLE IF EXISTS clientevent_staging_distinct;
        |CREATE TABLE clientevent_staging_distinct AS
        |-- INSERT overwrite TABLE clientevent_staging_distinct
        |SELECT DISTINCT event_source,
        |                event_date_pst,
        |                event_unixtime,
        |                event_uuid,
        |                event_logger,
        |                --widget_id,
        |                --session_id,
        |                --connection_id,
        |                --stream_id,
        |                --widget_type,
        |                --partner_id,
        |                CASE lower(action)
        |                    WHEN 'connect'
        |                        THEN if(logVersion is not NULL and trim(logVersion) <> '', connectionid, widget_id)
        |                    WHEN 'publish'
        |                        THEN if(logVersion is not NULL and trim(logVersion) <> '', streamid, widget_id)
        |                    WHEN 'subscribe'
        |                        THEN if(logVersion is not NULL and trim(logVersion) <> '', subscriberid, widget_id)
        |                    ELSE
        |                       --check for new logging else widg = widget_id
        |                       if(logVersion is not NULL and trim(logVersion) <> '',
        |                           --connectionid is null => widg=null
        |                           if(connectionId is NULL or trim(connectionId) = '', NULL,
        |                               --if conn not null & stream is null then widget=connectionid
        |                               if(streamId is NULL or trim(stream_id) = '', connectionId,
        |                                   -- (con,strm) is null => widget=streamid else sub
        |                                   if(subscriberId is NULL or trim(subscriberId) = '',streamId, subscriberId))),
        |                         widget_id)
        |                END widget_id,
        |
        |                if(logVersion is not NULL and trim(logVersion) <> '', sessionId,   session_id)     session_id,
        |                if(logVersion is not NULL and trim(logVersion) <> '', connectionId,connection_id) connection_id,
        |                if(logVersion is not NULL and trim(logVersion) <> '', streamId,    stream_id)     stream_id,
        |
        |                CASE lower(action)
        |                   WHEN 'connect'
        |                       THEN if(logVersion is not NULL and trim(logVersion) <> '', 'Controller', widget_type)
        |                   WHEN 'publish'
        |                       THEN if(logVersion is not NULL and trim(logVersion) <> '', 'Publisher',  widget_type)
        |                   WHEN 'subscribe'
        |                       THEN if(logVersion is not NULL and trim(logVersion) <> '', 'Subscriber', widget_type)
        |                   ELSE
        |                      if(logVersion is not NULL and trim(logVersion) <> '',
        |                         --connectionid is null => widget_type is null
        |                         if(connectionId is NULL or trim(connectionId) = '', NULL,
        |                             --if conn not null & streamid null => widget_type = controller
        |                             if(streamId is NULL or trim(stream_id) = '','   Controller',
        |                                 --if (conn, strm) not null & sub is null => publisher else subscriber
        |                                 if(subscriberId is NULL or subscriberId = '','Publisher', 'Subscriber'))),
        |                       widget_type)
        |                END widget_type,
        |
        |                if(logVersion is not NULL and trim(logVersion) <> '', partnerId, partner_id)  partner_id,
        |
        |                guid,
        |                source,
        |                action,
        |                section,
        |                variation,
        |                payload_type,
        |                payload,
        |                build,
        |                event_remote_ip,
        |                stream_type,
        |                to_date(event_date_pst) AS dt,
        |                country,
        |                region,
        |                city,
        |                latitude,
        |                longitude,
        |                logVersion,
        |                clientSystemTime,
        |                clientVersion,
        |                subscriberId,
        |                event_user_agent,
        |                connectionId,
        |                streamId,
        |                partnerId,
        |                sessionId,
        |                browsername,
        |                devicemodel,
        |                os,
        |                systemname ,
        |                systemversion,
        |                networkstatus,
        |                attemptduration,
        |                failurereason,
        |                failurecode,
        |                failuremessage,
        |                messagingserver,
        |                apiserver,
        |                features,
        |                p2p
        |FROM serverlog
        |WHERE dc >= regexp_replace(${DATECUT}, '-', '/')
        |   AND to_date(event_date_pst) >= ${DATECUT}
        |   AND event_type = 'ClientEvent';
        |
        |  FROM serverlog
        |INSERT overwrite TABLE clientevent_invalid partition (dt)
        |SELECT clientevent_staging_distinct.event_source,
        |       clientevent_staging_distinct.event_date_pst,
        |       clientevent_staging_distinct.event_unixtime,
        |       clientevent_staging_distinct.event_uuid,
        |       clientevent_staging_distinct.event_logger,
        |       clientevent_staging_distinct.widget_id,
        |       clientevent_staging_distinct.session_id,
        |       clientevent_staging_distinct.connection_id,
        |       clientevent_staging_distinct.stream_id,
        |       clientevent_staging_distinct.widget_type,
        |       clientevent_staging_distinct.partner_id,
        |       clientevent_staging_distinct.guid,
        |       clientevent_staging_distinct.source,
        |       clientevent_staging_distinct.action,
        |       clientevent_staging_distinct.section,
        |       clientevent_staging_distinct.variation,
        |       clientevent_staging_distinct.payload_type,
        |       clientevent_staging_distinct.payload,
        |       clientevent_staging_distinct.build,
        |       clientevent_staging_distinct.event_remote_ip,
        |       clientevent_staging_distinct.stream_type,
        |       clientevent_staging_distinct.country,
        |       clientevent_staging_distinct.region,
        |       clientevent_staging_distinct.city,
        |       clientevent_staging_distinct.latitude,
        |       clientevent_staging_distinct.longitude,
        |       clientevent_staging_distinct.logVersion,
        |       clientevent_staging_distinct.clientSystemTime,
        |       clientevent_staging_distinct.clientVersion,
        |       clientevent_staging_distinct.subscriberId,
        |       clientevent_staging_distinct.event_user_agent,
        |       clientevent_staging_distinct.connectionId,
        |       clientevent_staging_distinct.streamId,
        |       clientevent_staging_distinct.partnerId,
        |       clientevent_staging_distinct.sessionId,
        |       clientevent_staging_distinct.browsername,
        |       clientevent_staging_distinct.devicemodel,
        |       clientevent_staging_distinct.os,
        |       clientevent_staging_distinct.systemname,
        |       clientevent_staging_distinct.systemversion,
        |       clientevent_staging_distinct.networkstatus,
        |       clientevent_staging_distinct.attemptduration,
        |       clientevent_staging_distinct.failurereason,
        |       clientevent_staging_distinct.failurecode,
        |       clientevent_staging_distinct.failuremessage,
        |       clientevent_staging_distinct.messagingserver,
        |       clientevent_staging_distinct.apiserver,
        |       clientevent_staging_distinct.features,
        |       clientevent_staging_distinct.p2p,
        |       clientevent_staging_distinct.dt
        |WHERE (clientevent_staging_distinct.event_source IS NOT NULL
        |       AND length(clientevent_staging_distinct.event_source)>20)
        |  OR (clientevent_staging_distinct.event_logger IS NOT NULL
        |      AND length(clientevent_staging_distinct.event_logger)>40)
        |  OR (clientevent_staging_distinct.guid IS NOT NULL
        |      AND length(clientevent_staging_distinct.guid)>50)
        |  OR (clientevent_staging_distinct.session_id IS NOT NULL
        |      AND length(clientevent_staging_distinct.session_id)>512)
        |  OR (clientevent_staging_distinct.connection_id IS NOT NULL
        |      AND length(clientevent_staging_distinct.connection_id)>40)
        |  OR (length(clientevent_staging_distinct.stream_id)>40)
        |  OR (clientevent_staging_distinct.widget_type IS NOT NULL
        |      AND length(clientevent_staging_distinct.widget_type)>20)
        |  OR (clientevent_staging_distinct.action IS NOT NULL
        |      AND length(clientevent_staging_distinct.action)>255)
        |  OR (clientevent_staging_distinct.section IS NOT NULL
        |      AND length(clientevent_staging_distinct.section)>255)
        |  OR (clientevent_staging_distinct.variation IS NOT NULL
        |      AND length(clientevent_staging_distinct.variation)>255)
        |  OR (clientevent_staging_distinct.payload_type IS NOT NULL
        |      AND length(clientevent_staging_distinct.payload_type)>255)
        |  OR (clientevent_staging_distinct.build IS NOT NULL
        |      AND length(clientevent_staging_distinct.build)>255)
        |  OR (clientevent_staging_distinct.partner_id IS NOT NULL
        |      AND clientevent_staging_distinct.partner_id  <> ''
        |      AND cast(clientevent_staging_distinct.partner_id as int) IS NULL)
        |  OR (clientevent_staging_distinct.partner_id IS NOT NULL
        |      AND clientevent_staging_distinct.partner_id like '%-%')
        |  OR (clientevent_staging_distinct.partner_id IS NOT NULL
        |      AND clientevent_staging_distinct.partner_id like '%.%')
        |
        |
        |
        |INSERT overwrite TABLE clientevent_logs partition (dt)
        |  SELECT clientevent_staging_distinct.event_source,
        |         clientevent_staging_distinct.event_date_pst,
        |         clientevent_staging_distinct.event_unixtime,
        |         clientevent_staging_distinct.event_uuid,
        |         clientevent_staging_distinct.event_logger,
        |         clientevent_staging_distinct.widget_id,
        |         clientevent_staging_distinct.session_id,
        |         clientevent_staging_distinct.connection_id,
        |         clientevent_staging_distinct.stream_id,
        |         clientevent_staging_distinct.widget_type,
        |         (case when clientevent_staging_distinct.partner_id = '' then null else clientevent_staging_distinct.partner_id end) partner_id,
        |         clientevent_staging_distinct.guid,
        |         clientevent_staging_distinct.source,
        |         clientevent_staging_distinct.action,
        |         clientevent_staging_distinct.section,
        |         clientevent_staging_distinct.variation,
        |         clientevent_staging_distinct.payload_type,
        |         clientevent_staging_distinct.payload,
        |         clientevent_staging_distinct.build,
        |         reflect('org.apache.commons.codec.digest.DigestUtils', 'sha256Hex', event_remote_ip) as event_remote_ip,
        |         clientevent_staging_distinct.stream_type,
        |         clientevent_staging_distinct.country,
        |         clientevent_staging_distinct.region,
        |         clientevent_staging_distinct.city,
        |         clientevent_staging_distinct.latitude,
        |         clientevent_staging_distinct.longitude,
        |         clientevent_staging_distinct.logVersion,
        |         clientevent_staging_distinct.clientSystemTime,
        |         clientevent_staging_distinct.clientVersion,
        |         clientevent_staging_distinct.subscriberId,
        |         clientevent_staging_distinct.event_user_agent,
        |         clientevent_staging_distinct.connectionId,
        |         clientevent_staging_distinct.streamId,
        |         clientevent_staging_distinct.partnerId,
        |         clientevent_staging_distinct.sessionId,
        |         clientevent_staging_distinct.browsername,
        |         clientevent_staging_distinct.devicemodel,
        |         clientevent_staging_distinct.os,
        |         clientevent_staging_distinct.systemname,
        |         clientevent_staging_distinct.systemversion,
        |         clientevent_staging_distinct.networkstatus,
        |         clientevent_staging_distinct.attemptduration,
        |         clientevent_staging_distinct.failurereason,
        |         clientevent_staging_distinct.failurecode,
        |         clientevent_staging_distinct.failuremessage,
        |         clientevent_staging_distinct.messagingserver,
        |         clientevent_staging_distinct.apiserver,
        |         clientevent_staging_distinct.features,
        |         clientevent_staging_distinct.p2p,
        |         clientevent_staging_distinct.dt
        |         WHERE (clientevent_staging_distinct.event_source IS NOT NULL
        |  AND length(clientevent_staging_distinct.event_source)<=20)
        |  AND (clientevent_staging_distinct.event_logger IS NOT NULL
        |       AND length(clientevent_staging_distinct.event_logger)<=40)
        |  AND (clientevent_staging_distinct.guid IS NULL
        |       OR (clientevent_staging_distinct.guid IS NOT NULL
        |           AND length(clientevent_staging_distinct.guid)<=50))
        |  AND (clientevent_staging_distinct.session_id IS NULL
        |       OR (clientevent_staging_distinct.session_id IS NOT NULL
        |           AND length(clientevent_staging_distinct.session_id)<=512))
        |  AND (clientevent_staging_distinct.connection_id IS NULL
        |       OR (clientevent_staging_distinct.connection_id IS NOT NULL
        |           AND length(clientevent_staging_distinct.connection_id)<=40))
        |  AND (clientevent_staging_distinct.stream_id IS NULL
        |       OR (clientevent_staging_distinct.stream_id IS NOT NULL
        |           AND length(clientevent_staging_distinct.stream_id)<=40))
        |  AND (clientevent_staging_distinct.widget_type IS NULL
        |       OR ((clientevent_staging_distinct.widget_type IS NOT NULL
        |            AND length(clientevent_staging_distinct.widget_type)<=20)))
        |  AND (clientevent_staging_distinct.action IS NULL
        |       OR (clientevent_staging_distinct.action IS NOT NULL
        |           AND length(clientevent_staging_distinct.action)<=255))
        |  AND (clientevent_staging_distinct.section IS NULL
        |       OR (clientevent_staging_distinct.section IS NOT NULL
        |           AND length(clientevent_staging_distinct.section)<=255))
        |  AND (clientevent_staging_distinct.variation IS NULL
        |       OR (clientevent_staging_distinct.variation IS NOT NULL
        |           AND length(clientevent_staging_distinct.variation)<=255))
        |  AND (clientevent_staging_distinct.payload_type IS NULL
        |       OR (clientevent_staging_distinct.payload_type IS NOT NULL
        |           AND length(clientevent_staging_distinct.payload_type)<=255))
        |  AND (clientevent_staging_distinct.build IS NULL
        |       OR (clientevent_staging_distinct.build IS NOT NULL
        |           AND length(clientevent_staging_distinct.build)<=255))
        |  AND ((clientevent_staging_distinct.partner_id IS NULL
        |      OR clientevent_staging_distinct.partner_id = '')
        |      OR (clientevent_staging_distinct.partner_id IS NOT NULL
        |           AND (cast(clientevent_staging_distinct.partner_id as int) IS NOT NULL
        |               AND clientevent_staging_distinct.partner_id not like '%-%'
        |               AND clientevent_staging_distinct.partner_id not like '%.%')));
        | """.stripMargin ::
      """set hive.exec.dynamic.partition.mode=nonstrict ;
        |set hive.exec.dynamic.partition=true;
        |
        |INSERT OVERWRITE TABLE SuperTable partition (dt)
        |SELECT partner_id,
        |       max(ConcurrentTwoSubscriberSessions) MaxConcurrentTwoSubscriberSessions,
        |       dt
        |FROM
        |
        |    (SELECT partner_id, dt, count(distinct session_id) ConcurrentTwoSubscriberSessions,
        |            TimeHour, TimeMinute
        |     FROM
        |
        |          (SELECT partner_id, dt, session_id, count(distinct widget_id) ct,
        |                  hour(event_date_pst) TimeHour, minute(event_date_pst) TimeMinute
        |           FROM logslogs
        |           WHERE widget_type = 'Subscriber' and dt  = ${DATECUT}  and
        |                 partner_id  in (1,2,3) and
        |                 widget_type = 'Subscriber'
        |          GROUP BY partner_id,session_id, dt, hour(event_date_pst),
        |                   minute(event_date_pst)) ttt
        |
        |    WHERE ct = 2
        |    GROUP BY partner_id,dt,TimeHour,TimeMinute) tt
        |
        |GROUP BY partner_id, dt;
        | """.stripMargin ::
        """set hive.exec.parallel=true;
          |
          |-- servergeolocation likely out-of-date. Need to update datacenter and discard distances, lats, long, etc.
          |-- gets the messagingServer on the SesssionInfo (GSI) action. messagingServer was
          |-- embedded in json, hence the `get_json_object`.
          |
          |
          |ALTER TABLE lolTable DROP IF EXISTS PARTITION(dt=${DATECUT});
          |INSERT OVERWRITE TABLE lolTable PARTITION(dt=${DATECUT})
          |SELECT session_id, connection_id, guid,
          |       max(get_json_object(payload, '$.messagingServer')) messagingserver,
          |       sl.datacenter
          |FROM logslogs cl
          |LEFT OUTER JOIN lolGeo sl
          |ON get_json_object(cl.payload, '$.messagingServer') = concat(sl.hostname, '.tokbox.com')
          |WHERE cl.dt = ${DATECUT} and
          |      cl.logversion is not null  and
          |      cl.action = 'SessionInfo' and
          |      get_json_object(cl.payload, '$.messagingServer') is not null and
          |      cl.payload not like '%aa%' and
          |      (trim(session_id) <> '' and session_id is not null and trim(session_id) <> 'null') and
          |
          |      (trim(connection_id) <> '' and connection_id is not null and
          |             lower(trim(connection_id)) not rlike 'null|connection') and
          |
          |      decode_session_id(session_id) is not null and
          |
          |      logversion is not null and trim(logversion) <> ''
          |GROUP BY session_id, connection_id, guid, sl.datacenter, sl.latitude, sl.longitude,
          |         cl.dt;""".stripMargin ::
      Nil
}
