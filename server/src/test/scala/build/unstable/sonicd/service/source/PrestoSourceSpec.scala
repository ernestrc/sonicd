package build.unstable.sonicd.service.source

import akka.actor._
import akka.http.scaladsl.model.{ContentTypes, HttpRequest}
import akka.stream.actor.ActorPublisherMessage.Cancel
import akka.stream.actor.{ActorPublisher, ActorPublisherMessage}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.testkit.{CallingThreadDispatcher, ImplicitSender, TestActorRef, TestKit}
import build.unstable.sonic.JsonProtocol._
import build.unstable.sonic.model.{OutputChunk, Query, QueryProgress, RequestContext}
import build.unstable.sonicd.model._
import build.unstable.sonicd.source.Presto.{ColMeta, QueryResults, StatementStats}
import build.unstable.sonicd.source.http.HttpSupervisor.HttpRequestCommand
import build.unstable.sonicd.source.{Presto, PrestoPublisher}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import spray.json._

import scala.concurrent.Await
import scala.concurrent.duration._

class PrestoSourceSpec(_system: ActorSystem)
  extends TestKit(_system) with WordSpecLike
    with Matchers with BeforeAndAfterAll with ImplicitSender
    with ImplicitSubscriber with HandlerUtils {

  import Fixture._

  override protected def afterAll(): Unit = {
    materializer.shutdown()
    TestKit.shutdownActorSystem(system)
  }

  def this() = this(ActorSystem("PrestoSourceSpec"))

  implicit val ctx: RequestContext = RequestContext("test-trace-id", None)

  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system))

  val mockConfig =
    s"""
       | {
       |  "port" : 9200,
       |  "url" : "unstable.build",
       |  "class" : "PrestoSource"
       | }
    """.stripMargin.parseJson.asJsObject

  val controller: TestActorRef[TestController] =
    TestActorRef(Props(classOf[TestController], self).
      withDispatcher(CallingThreadDispatcher.Id))

  def newPublisher(q: String, context: RequestContext = testCtx, watermark: Int = -1,
                   maxRetries: Int = 0, retryIn: FiniteDuration = 1.second, retryMultiplier: Int = 1,
                   retryErrors: Either[List[Long], Unit] = Left(List.empty),
                   dispatcher: String = CallingThreadDispatcher.Id): TestActorRef[PrestoPublisher] = {
    val query = new Query(Some(1L), Some("traceId"), None, q, mockConfig)
    val src = new PrestoSource(watermark, maxRetries, retryIn, retryMultiplier, retryErrors,
      self, query, controller.underlyingActor.context, context)
    val ref = TestActorRef[PrestoPublisher](src.publisher.withDispatcher(dispatcher))
    ActorPublisher(ref).subscribe(subs)
    watch(ref)
    ref
  }

  val query1 = """show catalogs"""
  val defaultCol = ColMeta("name", "varchar")
  val defaultColumns = Vector(defaultCol, defaultCol.copy(name = "name2"))
  val defaultRow: Vector[JsValue] = Vector(JsNumber(1), JsString("String"))
  val defaultData = Vector(defaultRow, defaultRow)
  val defaultStats = StatementStats.apply("FINISHED", true, 0, 0, 0, 0, 0, 0, 0, 0, 0)
  val defaultQueryFinished = QueryProgress(QueryProgress.Finished, defaultStats.totalSplits, Some(defaultStats.totalSplits), Some("splits"))

  def assertRequest(req: HttpRequest, query: String) = {
    req.method.value shouldBe "POST"
    req._4.contentType shouldBe ContentTypes.`text/plain(UTF-8)`
    val entity = req.entity
    val payload = Await.result(entity.toStrict(10.seconds), 10.seconds).data.utf8String
    assert(req._2.toString().endsWith("/statement"))
    assert(payload == query)
  }

  def sendNext(pub: ActorRef, status: String,
               nextUri: Option[String],
               partialCancelUri: Option[String] = None) = {
    pub ! QueryResults("", "", partialCancelUri, nextUri, Some(defaultColumns),
      Some(defaultData), defaultStats, None, None, None)
    defaultData.foreach { h ⇒
      pub ! ActorPublisherMessage.Request(1)
      expectMsg(OutputChunk(defaultRow))
    }
  }

  def completeSimpleStream(pub: ActorRef,
                           data: Vector[Vector[JsValue]] = defaultData,
                           columns: Option[Vector[ColMeta]] = Some(defaultColumns),
                           partialCancelUri: Option[String] = None,
                           nextUri: Option[String] = None) = {
    pub ! QueryResults("", "", partialCancelUri, nextUri,
      columns, Some(data), defaultStats, None, None, None)
    expectMsg(QueryProgress(QueryProgress.Started, 0, None, None))
    pub ! ActorPublisherMessage.Request(1)
    expectTypeMetadata()
    pub ! ActorPublisherMessage.Request(1)
    expectMsg(defaultQueryFinished)
    data.foreach { h ⇒
      pub ! ActorPublisherMessage.Request(1)
      expectMsg(OutputChunk(defaultRow))
    }
  }

  "PrestoSource" should {
    "run a simple query" in {
      val pub = newPublisher(query1)
      pub ! ActorPublisherMessage.Request(1)
      val httpCmd = expectMsgType[HttpRequestCommand]

      assertRequest(httpCmd.request, query1)

      expectStreamStarted()

      pub ! ActorPublisherMessage.Request(1)
      completeSimpleStream(pub)

      pub ! ActorPublisherMessage.Request(1)
      expectDone(pub)
    }


    "should extract type metadata from StatementStats" in {
      val pub = newPublisher(query1)
      pub ! ActorPublisherMessage.Request(1)
      val httpCmd = expectMsgType[HttpRequestCommand]

      assertRequest(httpCmd.request, query1)

      expectStreamStarted()
      pub ! ActorPublisherMessage.Request(1)

      val col1 = ColMeta("a", "boolean")
      val col2 = ColMeta("b", "bigint")
      val col3 = ColMeta("c", "double")
      val col4 = ColMeta("d", "varchar")
      val col5 = ColMeta("e", "varbinary")
      val col6 = ColMeta("f", "array")
      val col7 = ColMeta("g", "json")
      val col8 = ColMeta("h", "map")
      val col9 = ColMeta("i", "time")
      val col10 = ColMeta("j", "timestamp")
      val col11 = ColMeta("k", "date")

      val columns = Vector(col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11)

      pub ! ActorPublisherMessage.Request(1)
      expectMsg(QueryProgress(QueryProgress.Started, 0, None, None))

      pub ! QueryResults("", "", None, None,
        Some(columns), None, defaultStats, None, None, None)

      pub ! ActorPublisherMessage.Request(1)
      val meta = expectTypeMetadata()

      pub ! ActorPublisherMessage.Request(1)
      expectMsg(defaultQueryFinished)

      val (cols, types) = meta.typesHint.unzip

      assert(cols == columns.map(_.name))

      columns.zipWithIndex.foreach {
        case (ColMeta(n, "boolean"), i) ⇒ types(i).getClass.getSuperclass shouldBe classOf[JsBoolean]
        case (ColMeta(n, "bigint" | "double"), i) ⇒ types(i).getClass shouldBe classOf[JsNumber]
        case (ColMeta(n, "varchar" | "timestamp" | "date" | "time"), i) ⇒ types(i).getClass shouldBe classOf[JsString]
        case (ColMeta(n, "array" | "varbinary"), i) ⇒ types(i).getClass shouldBe classOf[JsArray]
        case (ColMeta(n, "json" | "map"), i) ⇒ types(i).getClass shouldBe classOf[JsObject]
        case (ColMeta(n, "panacea"), i) ⇒ types(i).getClass shouldBe classOf[JsString]
      }

      pub ! ActorPublisherMessage.Request(1)
      expectDone(pub)
    }

    "if query results state is not finished, it should use nextUri to get the next queryResults" in {
      val pub = newPublisher(query1)
      pub ! ActorPublisherMessage.Request(1)
      expectMsgType[HttpRequestCommand]

      expectStreamStarted()
      pub ! ActorPublisherMessage.Request(1)

      val stats1 = StatementStats.apply("STARTING", true, 0, 0, 0, 0, 0, 0, 0, 0, 0)

      expectQueryProgress(0, QueryProgress.Started, None, None)

      pub ! QueryResults("", "", None, Some("http://1"),
        Some(defaultColumns), None, stats1.copy(state = "RUNNING"), None, None, None)

      assert(pub.underlyingActor.lastQueryResults.nonEmpty)

      pub ! ActorPublisherMessage.Request(1)
      expectTypeMetadata()

      pub ! ActorPublisherMessage.Request(1)
      expectQueryProgress(0, QueryProgress.Running, Some(0), Some("splits"))

      assert(pub.underlyingActor.buffer.isEmpty)

      {
        val cmd = expectMsgType[HttpRequestCommand]
        assert(cmd.request._2.toString().endsWith("1"))

        pub ! QueryResults("", "", None, Some("http://2"),
          Some(defaultColumns), None, stats1.copy(completedSplits = 0,
            totalSplits = 1000), None, None, None)

        pub ! ActorPublisherMessage.Request(1)
        expectQueryProgress(0, QueryProgress.Running, Some(1000), Some("splits"))
      }
      {
        val cmd = expectMsgType[HttpRequestCommand]
        assert(cmd.request._2.toString().endsWith("2"))

        pub ! QueryResults("", "", None, Some("http://3"),
          Some(defaultColumns), None, stats1.copy(completedSplits = 100, totalSplits = 1000), None, None, None)

        pub ! ActorPublisherMessage.Request(1)
        expectQueryProgress(100, QueryProgress.Running, Some(1000), Some("splits"))
      }
      {
        val cmd = expectMsgType[HttpRequestCommand]
        assert(cmd.request._2.toString().endsWith("3"))

        pub ! QueryResults("", "", None, Some("http://4"),
          Some(defaultColumns), None, stats1.copy(completedSplits = 200, totalSplits = 1000), None, None, None)

        pub ! ActorPublisherMessage.Request(1)
        expectQueryProgress(100, QueryProgress.Running, Some(1000), Some("splits"))

      }
      {
        val cmd = expectMsgType[HttpRequestCommand]
        assert(cmd.request._2.toString().endsWith("4"))

        pub ! QueryResults("", "", None, Some("http://5"),
          Some(defaultColumns), None, stats1.copy(state = "RUNNING",
            completedSplits = 950, totalSplits = 1000), None, None, None)

        pub ! ActorPublisherMessage.Request(1)
        expectQueryProgress(750, QueryProgress.Running, Some(1000), Some("splits"))

      }
      {
        val cmd = expectMsgType[HttpRequestCommand]
        assert(cmd.request._2.toString().endsWith("5"))

        pub ! QueryResults("", "", None, Some("http://6"),
          Some(defaultColumns), None, stats1.copy(state = "RUNNING", completedSplits = 1000, totalSplits = 1000), None, None, None)

        pub ! ActorPublisherMessage.Request(1)
        expectQueryProgress(50, QueryProgress.Running, Some(1000), Some("splits"))

      }
      {
        /* 100 - 50 */
        val cmd = expectMsgType[HttpRequestCommand]
        assert(cmd.request._2.toString().endsWith("6"))

        pub ! QueryResults("", "", None, None,
          Some(defaultColumns), None, stats1.copy(state = "FINISHED", completedSplits = 1000, totalSplits = 1000), None, None, None)
      }

      pub ! ActorPublisherMessage.Request(1)
      expectMsg(QueryProgress(QueryProgress.Finished, 0, Some(1000), Some("splits")))

      pub ! ActorPublisherMessage.Request(1)
      expectDone(pub)
    }

    "should stop if user cancels" in {
      val pub = newPublisher(query1)

      pub ! ActorPublisherMessage.Request(1)
      val httpCmd = expectMsgType[HttpRequestCommand]

      expectStreamStarted()
      pub ! ActorPublisherMessage.Request(1)

      expectMsg(QueryProgress(QueryProgress.Started, 0, None, None))

      pub ! QueryResults("", "", Some("http://cancel"), None, Some(defaultColumns),
        None, defaultStats.copy(state = "RUNNING"), None, None, None)

      pub ! ActorPublisherMessage.Request(1)
      expectTypeMetadata()

      pub ! ActorPublisherMessage.Request(1)
      expectQueryProgress(0, QueryProgress.Running, Some(0), Some("splits"))

      pub ! Cancel
      expectTerminated(pub)
    }

    "should attempt to query ahead" in {
      val pub = newPublisher(query1, watermark = 5)
      pub ! ActorPublisherMessage.Request(1)
      expectMsgType[HttpRequestCommand]

      expectStreamStarted()
      pub ! ActorPublisherMessage.Request(1)

      val stats1 = StatementStats.apply("STARTING", true, 0, 0, 0, 0, 0, 0, 0, 0, 0)

      expectQueryProgress(0, QueryProgress.Started, None, None)

      pub ! QueryResults("", "", None, Some("http://1"),
        Some(defaultColumns), None, stats1, None, None, None)


      expectMsgType[HttpRequestCommand]
      pub ! ActorPublisherMessage.Request(1)
      expectTypeMetadata()

      pub ! ActorPublisherMessage.Request(1)
      expectQueryProgress(0, QueryProgress.Running, Some(0), Some("splits"))

      pub ! QueryResults("", "", None, Some("http://2"),
        Some(defaultColumns), None, stats1.copy(completedSplits = 200, totalSplits = 1000),
        None, None, None)

      //buffer 1, streamed = 0

      expectMsgType[HttpRequestCommand]
      pub ! ActorPublisherMessage.Request(1)
      expectQueryProgress(200, QueryProgress.Running, Some(1000), Some("splits"))

      pub ! QueryResults("", "", None, Some("http://3"),
        Some(defaultColumns), None, stats1.copy(completedSplits = 400, totalSplits = 1000), None, None, None)


      expectMsgType[HttpRequestCommand]

      pub ! QueryResults("", "", None, Some("http://3"),
        Some(defaultColumns), None, stats1.copy(completedSplits = 600, totalSplits = 1000), None, None, None)

      //buffer is 2 streamed is 0

      expectMsgType[HttpRequestCommand]

      pub ! QueryResults("", "", None, Some("http://3"),
        Some(defaultColumns), Some(Vector(defaultRow)),
        stats1.copy(completedSplits = 800, totalSplits = 1000), None, None, None)

      //buffer is 4 streamed is 0

      expectMsgType[HttpRequestCommand]

      pub ! QueryResults("", "", None, Some("http://3"),
        Some(defaultColumns), Some(defaultData),
        stats1.copy(state = "FINISHED", completedSplits = 1000, totalSplits = 1000),
        None, None, None)

      //buffer is 7 streamed is 0
      assert(pub.underlyingActor.totalDemand == 0)
      expectNoMsg()

      pub ! ActorPublisherMessage.Request(100)
      expectQueryProgress(200, QueryProgress.Running, Some(1000), Some("splits"))
      expectQueryProgress(200, QueryProgress.Running, Some(1000), Some("splits"))
      expectQueryProgress(200, QueryProgress.Running, Some(1000), Some("splits"))
      expectMsgType[OutputChunk]
      pub ! ActorPublisherMessage.Request(1)
      expectMsg(QueryProgress(QueryProgress.Finished, 0, Some(1000), Some("splits")))
      expectMsgType[OutputChunk]
      expectMsgType[OutputChunk]

      expectDone(pub)
    }

    "should retry up to a maximum of n retries if error is in 'retry-errors' list" in {
      val pub = newPublisher(query1, maxRetries = 3, retryIn = 1.millisecond, retryMultiplier = -1,
        retryErrors = Left(List(1234, 3434)))
      pub ! ActorPublisherMessage.Request(100)

      expectMsgType[HttpRequestCommand]

      expectStreamStarted()
      pub ! ActorPublisherMessage.Request(1)

      expectQueryProgress(0, QueryProgress.Started, None, None)

      val error = Presto.ErrorMessage("", 1234, "", "", Presto.FailureInfo("", Vector.empty, None))

      pub ! QueryResults("", "", None, None, None, None,
        defaultStats.copy(state = "FAILED"), Some(error), None, None)

      expectMsgType[HttpRequestCommand]

      pub ! QueryResults("", "", None, None, None, None,
        defaultStats.copy(state = "FAILED"), Some(error), None, None)

      expectMsgType[HttpRequestCommand]

      pub ! QueryResults("", "", None, None, None, None,
        defaultStats.copy(state = "FAILED"), Some(error.copy(errorCode = 3434)), None, None)

      expectMsgType[HttpRequestCommand]

      pub ! QueryResults("", "", None, None, None, None,
        defaultStats.copy(state = "FAILED"), Some(error), None, None)

      expectDone(pub, success = false)
    }

    "should retry up to a maximum of n retries if error is INTERNAL_ERROR or EXTERNAL and 'retry-errors' config is 'all'" in {
      val pub = newPublisher(query1, maxRetries = 3, retryIn = 1.millisecond,
        retryMultiplier = -1, retryErrors = Right.apply(()))
      pub ! ActorPublisherMessage.Request(100)

      expectMsgType[HttpRequestCommand]

      expectStreamStarted()
      pub ! ActorPublisherMessage.Request(1)

      expectQueryProgress(0, QueryProgress.Started, None, None)

      val error = Presto.ErrorMessage("", 1234, "", "INTERNAL_ERROR", Presto.FailureInfo("", Vector.empty, None))

      pub ! QueryResults("", "", None, None, None, None,
        defaultStats.copy(state = "FAILED"), Some(error), None, None)

      expectMsgType[HttpRequestCommand]

      pub ! QueryResults("", "", None, None, None, None,
        defaultStats.copy(state = "FAILED"), Some(error.copy(errorType = "EXTERNAL")), None, None)

      expectMsgType[HttpRequestCommand]

      pub ! QueryResults("", "", None, None, None, None,
        defaultStats.copy(state = "FAILED"), Some(error.copy(errorCode = 3434)), None, None)

      expectMsgType[HttpRequestCommand]

      pub ! QueryResults("", "", None, None, None, None,
        defaultStats.copy(state = "FAILED"), Some(error), None, None)

      expectDone(pub, success = false)
    }

    "should NOT retry if config 'retry-errors' is 'all' but error is not internal or external" in {
      val pub = newPublisher(query1, maxRetries = 3, retryIn = 1.millisecond,
        retryMultiplier = -1, retryErrors = Right.apply(()))

      pub ! ActorPublisherMessage.Request(100)

      expectMsgType[HttpRequestCommand]

      expectStreamStarted()

      expectQueryProgress(0, QueryProgress.Started, None, None)

      val error = Presto.ErrorMessage("", 1224, "", "USER_ERROR",
        Presto.FailureInfo("", Vector.empty, None))

      pub ! QueryResults("", "", None, None, None, None,
        defaultStats.copy(state = "FAILED"), Some(error), None, None)

      expectDone(pub, success = false)
    }

    "should NOT retry if config retries is 0" in {
      val pub = newPublisher(query1, maxRetries = 0, retryIn = 1.second)
      pub ! ActorPublisherMessage.Request(100)

      expectMsgType[HttpRequestCommand]

      expectStreamStarted()
      pub ! ActorPublisherMessage.Request(1)

      expectQueryProgress(0, QueryProgress.Started, None, None)

      val error = Presto.ErrorMessage("", 65540, "", "",
        Presto.FailureInfo("", Vector.empty, None))

      pub ! QueryResults("", "", None, None, None, None,
        defaultStats.copy(state = "FAILED"), Some(error), None, None)

      expectDone(pub, success = false)
    }
  }
}

//override supervisor
class PrestoSource(watermark: Int, maxRetries: Int, retryIn: FiniteDuration, retryMultiplier: Int,
                   retryErrors: Either[List[Long], Unit], implicitSender: ActorRef, query: Query,
                   actorContext: ActorContext, context: RequestContext)
  extends build.unstable.sonicd.source.PrestoSource(query, actorContext, context) {

  override def getSupervisor(name: String): ActorRef = implicitSender

  override lazy val publisher: Props = {
    //if no ES supervisor has been initialized yet for this ES cluster, initialize one
    val supervisor = getSupervisor(supervisorName)

    Props(classOf[PrestoPublisher], query.traceId.get, query.query, implicitSender, watermark,
      maxRetries, retryIn, retryMultiplier, retryErrors, context)
  }
}
