package build.unstable.sonicd.service.source

import akka.actor.{ActorContext, ActorRef, ActorSystem, Props}
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.stream.actor.{ActorPublisher, ActorPublisherMessage}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.testkit.{CallingThreadDispatcher, ImplicitSender, TestActorRef, TestKit}
import build.unstable.sonic.JsonProtocol._
import build.unstable.sonic._
import build.unstable.sonic.model.{OutputChunk, Query, RequestContext, TypeMetadata}
import build.unstable.sonicd.model._
import build.unstable.sonicd.source.http.HttpSupervisor.HttpRequestCommand
import build.unstable.sonicd.source.{ElasticSearch, ElasticSearchPublisher}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import spray.json._

import scala.concurrent.Await
import scala.concurrent.duration._

class ElasticSearchSourceSpec(_system: ActorSystem)
  extends TestKit(_system) with WordSpecLike
    with Matchers with BeforeAndAfterAll with ImplicitSender
    with ImplicitSubscriber with HandlerUtils {

  import Fixture._

  override protected def afterAll(): Unit = {
    materializer.shutdown()
    TestKit.shutdownActorSystem(system)
  }

  def this() = this(ActorSystem("ElasticSearchSourceSpec"))

  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system))

  val mockConfig =
    s"""
       | {
       |  "port" : 9200,
       |  "url" : "unstable.build",
       |  "class" : "ElasticSearchSource"
       | }
    """.stripMargin.parseJson.asJsObject

  val controller: TestActorRef[TestController] =
    TestActorRef(Props(classOf[TestController], self).
      withDispatcher(CallingThreadDispatcher.Id))

  def newPublisher(q: String, context: RequestContext = testCtx,
                   watermark: Long = 10, querySize: Long = 100): TestActorRef[ElasticSearchPublisher] = {
    val query = new Query(Some(1L), Some("traceId"), None, q, mockConfig)
    val src = new ElasticSearchSource(querySize, watermark, self, query, controller.underlyingActor.context, context)
    val ref = TestActorRef[ElasticSearchPublisher](src.publisher.withDispatcher(CallingThreadDispatcher.Id))
    ActorPublisher(ref).subscribe(subs)
    watch(ref)
    ref
  }

  def getQueryResults(h: Vector[ElasticSearch.Hit]): ElasticSearch.QueryResults = {
    val shards = ElasticSearch.Shards(10, 10, 10)
    val hits = ElasticSearch.Hits(h.length, 1, h)
    ElasticSearch.QueryResults(Some("traceId"), 10, timed_out = false, shards, hits)
  }

  val defaultHit = """{"a":"b"}""".parseJson.asJsObject
  val defaultHitValue = defaultHit.fields.head._2

  def getHit(data: JsObject = defaultHit) =
    ElasticSearch.Hit("", "", "", 10.0f, data)

  val query1 = """{"query":{"term":{"event_source":{"value":"raven"}}}}"""
  val queryWithType = """{"_type": "complicatedType", "query":{"term":{"event_source":{"value":"raven"}}}}"""
  val queryWithIndex = """{"_index": "complicatedIndex", "query":{"term":{"event_source":{"value":"raven"}}}}"""
  val queryWithFrom = """{"query":{"term":{"event_source":{"value":"raven"}}},"from":5}"""


  def completeSimpleStream(pub: ActorRef, hits: Int = 1) = {
    pub ! getQueryResults((0 until hits).map(_ ⇒ getHit()).toVector)
    expectTypeMetadata()
    (0 until hits).foreach { h ⇒
      pub ! ActorPublisherMessage.Request(1)
      expectMsg(OutputChunk(Vector(defaultHitValue)))
    }
  }

  def assertPayload(entity: HttpEntity, sizeShould: Long, fromShould: Long) = {
    val payload = Await.result(entity.toStrict(10.seconds), 10.seconds).data.utf8String
    val obj = payload.parseJson.asJsObject.fields

    obj("size") shouldBe JsNumber(sizeShould)
    obj("from") shouldBe JsNumber(fromShould)

    obj.get("_type") shouldBe None
    obj.get("_index") shouldBe None
  }

  "ElasticSearchSource" should {
    "run a simple query" in {
      val querySize = 100
      val pub = newPublisher(query1, querySize = querySize)
      pub ! ActorPublisherMessage.Request(1)
      val httpCmd = expectMsgType[HttpRequestCommand]

      httpCmd.request.method.value shouldBe "POST"
      httpCmd.request._4.contentType shouldBe ContentTypes.`application/json`
      assert(httpCmd.request._2.toRelative.toString().endsWith("/_search"))
      assertPayload(httpCmd.request._4, querySize, 0)

      expectStreamStarted()
      pub ! ActorPublisherMessage.Request(1)

      completeSimpleStream(pub)

      pub ! ActorPublisherMessage.Request(1)
      expectDone(pub)
    }

    "emit metadata if schema changes" in {
      val querySize = 100
      val pub = newPublisher(query1, querySize = querySize)
      pub ! ActorPublisherMessage.Request(1)
      expectMsgType[HttpRequestCommand]

      expectStreamStarted()
      pub ! ActorPublisherMessage.Request(1000)

      val hit1Data = """{"a": "b"}""".parseJson.asJsObject
      val hit1 = ElasticSearch.Hit("", "", "", 0, hit1Data)
      val hit2Data = """{"s": "b"}""".parseJson.asJsObject
      val hit2 = ElasticSearch.Hit("", "", "", 0, hit2Data)

      pub ! getQueryResults(Vector(hit1, hit1, hit2))
      val meta1 = expectTypeMetadata()
      expectMsg(OutputChunk(Vector("b")))

      // same schema
      expectMsg(OutputChunk(Vector("b")))

      // diff schema
      val meta2 = expectTypeMetadata()
      assert(meta1.typesHint != meta2.typesHint)
      assert(meta2.typesHint.contains(("a", JsString("b"))))
      assert(meta2.typesHint.contains(("s", JsString("b"))))
      expectMsg(OutputChunk(Vector[JsValue](JsNull, JsString("b"))))

      expectDone(pub)
    }

    "if query does not have an index, uri index should be set to _all" in {
      val pub = newPublisher(query1)
      pub ! ActorPublisherMessage.Request(1)
      val httpCmd = expectMsgType[HttpRequestCommand]

      assert(httpCmd.request._2.toString().contains("_all"))

      expectStreamStarted()
      pub ! ActorPublisherMessage.Request(1)
      completeSimpleStream(pub)

      pub ! ActorPublisherMessage.Request(1)
      expectDone(pub)
    }

    "extract _type from query and add it to the query uri" in {
      val pub = newPublisher(queryWithType)
      pub ! ActorPublisherMessage.Request(1)
      val httpCmd = expectMsgType[HttpRequestCommand]

      assert(httpCmd.request._2.toString().contains("complicatedType"))
      httpCmd.request._4.toString() should not contain "complicatedType"

      expectStreamStarted()
      pub ! ActorPublisherMessage.Request(1)
      completeSimpleStream(pub)

      pub ! ActorPublisherMessage.Request(1)
      expectDone(pub)
    }

    "extract _index from query and add it to the query uri" in {
      val pub = newPublisher(queryWithIndex)
      pub ! ActorPublisherMessage.Request(1)
      val httpCmd = expectMsgType[HttpRequestCommand]

      assert(httpCmd.request._2.toString().contains("complicatedIndex"))
      httpCmd.request._4.toString() should not contain "complicatedIndex"

      expectStreamStarted()
      pub ! ActorPublisherMessage.Request(1)
      completeSimpleStream(pub)

      pub ! ActorPublisherMessage.Request(1)
      expectDone(pub)
    }

    "if query has a 'size', limit es query to that and stream exactly that number of elements" in {
      val queryWithSize = """{"query":{"term":{"event_source":{"value":"raven"}}},"size":5}"""
      val pub = newPublisher(queryWithSize)
      pub ! ActorPublisherMessage.Request(1)
      val httpCmd = expectMsgType[HttpRequestCommand]

      //assert that size is one
      assertPayload(httpCmd.request._4, 5, 0)

      expectStreamStarted()
      pub ! ActorPublisherMessage.Request(1)

      //complete stream
      completeSimpleStream(pub, 3)
      pub ! ActorPublisherMessage.Request(1)
      expectDone(pub)
    }

    "if query has a 'from' respect and pass it to ES" in {
      val querySize = 100
      val pub = newPublisher(queryWithFrom, querySize = querySize)
      pub ! ActorPublisherMessage.Request(1)
      val httpCmd = expectMsgType[HttpRequestCommand]

      //assert that from is correct
      assertPayload(httpCmd.request._4, querySize, 5)

      expectStreamStarted()
      pub ! ActorPublisherMessage.Request(1)

      //complete stream
      completeSimpleStream(pub)
      pub ! ActorPublisherMessage.Request(1)
      expectDone(pub)
    }

    def testNoQueryAhead(pub: TestActorRef[ElasticSearchPublisher],
                         querySize: Long, totalHits: Long,
                         userSetSize: Option[Long] = None,
                         userSetFrom: Option[Long] = None) {
      val from = userSetFrom getOrElse 0L
      var fetched = 0L
      val effectiveTotal = userSetSize getOrElse totalHits
      val shards = ElasticSearch.Shards(10, 10, 10)
      val values = Vector(defaultHitValue)
      val hits = (0 until querySize.toInt).map(_ ⇒ getHit()).toVector
      val h = ElasticSearch.Hits(totalHits, 0.24f, hits)
      val result = ElasticSearch.QueryResults(Some("traceId"), 1, timed_out = false, shards, h)


      var meta: Option[TypeMetadata] = None

      while (from + fetched < effectiveTotal) {
        pub ! ActorPublisherMessage.Request(1)

        val httpCmd = expectMsgType[HttpRequestCommand]
        assertPayload(httpCmd.request._4, querySize, fetched + from)
        pub ! result
        fetched += querySize

        if (meta.isEmpty) {
          meta = Some(expectTypeMetadata())
          pub ! ActorPublisherMessage.Request(1)
        }

        var streamed = 0
        while (streamed < hits.size) {

          val chunk = expectMsg(OutputChunk(values))
          streamed += chunk.data.elements.size
          pub ! ActorPublisherMessage.Request(1)
        }
      }

      fetched shouldBe effectiveTotal
    }

    "chunkify requests to elastic search" in {
      val querySize = 10
      val watermark = 0
      //no query ahead
      val pub = newPublisher(query1, querySize = querySize, watermark = watermark)

      //query has totalHits 100 but returned was 10 (as requested with querySize)
      val totalHits = 100

      pub ! ActorPublisherMessage.Request(1)
      expectStreamStarted()

      testNoQueryAhead(pub, querySize, totalHits)
      expectDone(pub)
    }

    "chunkify requests to elastic search when user sets a 'from'" in {
      val querySize = 10
      val watermark = 0
      //no query ahead
      val userSetFrom = 5
      val pub = newPublisher(queryWithFrom, querySize = querySize, watermark = watermark)

      //query has totalHits 100 but returned was 10 (as requested with querySize)
      val totalHits = 100

      pub ! ActorPublisherMessage.Request(1)
      expectStreamStarted()
      testNoQueryAhead(pub, querySize, totalHits, userSetFrom = Some(userSetFrom))
      expectDone(pub)
    }

    "if user sets limit to be bigger than our desired fetch size, it should chunkify fetching" in {
      val queryWithSize = """{"query":{"term":{"event_source":{"value":"raven"}}},"size":50}"""
      val userSetSize = 50
      //user's desired size
      val querySize = 10
      //our desired size
      val watermark = 0
      //no query ahead
      val pub = newPublisher(queryWithSize, querySize = querySize, watermark = watermark)

      //query has totalHits 100 but returned was 10 (as requested with querySize)
      val totalHits = 100

      pub ! ActorPublisherMessage.Request(1)
      expectStreamStarted()
      testNoQueryAhead(pub, querySize, totalHits, Some(userSetSize))
      expectDone(pub)
    }

    "queryAhead depending on buffer size and configured watermark" in {
      val queryWithSize = """{"query":{"term":{"event_source":{"value":"raven"}}},"size":10}"""
      val querySize = 4
      val watermark = 2
      //query when buffer is smaller than 5
      val pub = newPublisher(queryWithSize, querySize = querySize, watermark = watermark)

      //query has totalHits 100 but returned was 10 (as requested with querySize)
      val totalHits = 100

      val shards = ElasticSearch.Shards(10, 10, 10)
      val values = Vector(defaultHitValue)
      val hits = (0 until querySize.toInt).map(_ ⇒ getHit()).toVector
      val h = ElasticSearch.Hits(totalHits, 0.24f, hits)
      val result = ElasticSearch.QueryResults(Some("traceId"), 1, timed_out = false, shards, h)

      pub ! ActorPublisherMessage.Request(1)

      val httpCmd1 = expectMsgType[HttpRequestCommand]
      assertPayload(httpCmd1.request._4, querySize, 0)
      pub ! result

      expectStreamStarted()
      pub ! ActorPublisherMessage.Request(1)

      expectTypeMetadata()
      pub ! ActorPublisherMessage.Request(1)

      expectMsg(OutputChunk(values))
      //buffer is 3, streamed is 1
      pub ! ActorPublisherMessage.Request(1)

      expectMsg(OutputChunk(values))
      //buffer is 2, streamed is 2
      pub ! ActorPublisherMessage.Request(1)

      expectMsg(OutputChunk(values))
      //buffer is 1, streamed is 3
      pub ! ActorPublisherMessage.Request(1)

      //time to query ahead
      val httpCmd2 = expectMsgType[HttpRequestCommand]
      assertPayload(httpCmd2.request._4, querySize, querySize)
      pub ! result

      expectMsg(OutputChunk(values))
      //buffer is 4, streamed is 4
      pub ! ActorPublisherMessage.Request(1)

      expectMsg(OutputChunk(values))
      //buffer is 3, streamed is 5
      pub ! ActorPublisherMessage.Request(1)

      expectMsg(OutputChunk(values))
      //buffer is 2, streamed is 6
      pub ! ActorPublisherMessage.Request(1)

      expectMsg(OutputChunk(values))
      //buffer is 1, streamed is 7
      pub ! ActorPublisherMessage.Request(1)

      //time to query ahead
      val httpCmd3 = expectMsgType[HttpRequestCommand]
      assertPayload(httpCmd3.request._4, 2, querySize * 2)
      //only 2 hits left
      pub ! ElasticSearch.QueryResults(Some("traceId"), 1, timed_out = false, shards,
        ElasticSearch.Hits(totalHits, 0.24f, Vector(getHit(), getHit())))

      expectMsg(OutputChunk(values))
      //buffer is 2, streamed is 8
      pub ! ActorPublisherMessage.Request(1)

      expectMsg(OutputChunk(values))
      //buffer is 1, streamed is 9
      pub ! ActorPublisherMessage.Request(1)

      expectMsg(OutputChunk(values))
      //buffer is 0, streamed is 10
      pub ! ActorPublisherMessage.Request(1)

      expectDone(pub)
    }
  }
}

//override supervisor
class ElasticSearchSource(querySize: Long, watermark: Long, implicitSender: ActorRef, query: Query, actorContext: ActorContext, context: RequestContext)
  extends build.unstable.sonicd.source.ElasticSearchSource(query, actorContext, context) {
  override def getSupervisor(name: String): ActorRef = implicitSender

  override lazy val publisher: Props = {
    //if no ES supervisor has been initialized yet for this ES cluster, initialize one
    val supervisor = getSupervisor(supervisorName)

    Props(classOf[ElasticSearchPublisher], query.traceId.get, esQuery,
      index, typeHint, querySize, supervisor, watermark, context)
  }
}
