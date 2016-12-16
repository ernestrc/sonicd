package build.unstable.sonicd.service.source

import akka.actor._
import akka.stream.actor.{ActorPublisher, ActorPublisherMessage}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.testkit.{CallingThreadDispatcher, ImplicitSender, TestActorRef, TestKit}
import build.unstable.sonic.JsonProtocol._
import build.unstable.sonic.model._
import build.unstable.sonicd.model.Fixture._
import build.unstable.sonicd.model._
import build.unstable.sonicd.source.Composer._
import build.unstable.sonicd.source.{Composer, ComposerPublisher}
import build.unstable.sonicd.system.actor.SonicdController.SonicdQuery
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import spray.json._

import scala.concurrent.duration._

class ComposerSpec(_system: ActorSystem)
  extends TestKit(_system) with WordSpecLike
    with Matchers with BeforeAndAfterAll with ImplicitSender
    with ImplicitSubscriber with HandlerUtils {


  override protected def afterAll(): Unit = {
    materializer.shutdown()
    TestKit.shutdownActorSystem(system)
  }

  def this() = this(ActorSystem("ComposerSourceSpec"))

  implicit val ctx: RequestContext = RequestContext("test-trace-id", None)

  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system))

  val controller: TestActorRef[TestController] =
    TestActorRef(Props(classOf[TestController], self).
      withDispatcher(CallingThreadDispatcher.Id))

  def newPublisher(q: String, queries: List[ComposedQuery],
                   strategy: ComposeStrategy,
                   bufferSize: Int = 256,
                   placeholder: Option[String] = None,
                   context: RequestContext = testCtx,
                   dispatcher: String = CallingThreadDispatcher.Id): TestActorRef[ComposerPublisher] = {
    implicit val jsonFormat = Composer.getComposedQueryJsonFormat(placeholder, q, context)
    val mockConfig = JsObject(
      Map(
        "strategy" → strategy.toJson,
        "buffer" → JsNumber(bufferSize),
        "queries" → queries.toJson
      )
    )
    val query = new Query(Some(1L), Some("traceId"), None, q, mockConfig)
    val src = new Composer(query, controller.underlyingActor.context, context)
    val ref = TestActorRef[ComposerPublisher](src.publisher.withDispatcher(dispatcher))
    ActorPublisher(ref).subscribe(subs)
    watch(ref)
    ref
  }

  def completeSimpleStream() = {

  }

  "PrestoSource" should {

    val root = """10001"""
    val mockConfig = JsObject(Map(
      "class" → JsString("build.unstable.sonicd.service.MockSource")
    ))

    "concat two simple queries' streams" in {
      val query1 = Query.apply("10", mockConfig, None)
      val query2 = Query.apply("10", mockConfig, None)
      val pub = newPublisher(root, ComposedQuery(query1, 0, Some("test1")) ::
        ComposedQuery(query2, 0, Some("test2")) :: Nil, Composer.ConcatStrategy)

      // force instantiate underlying publishers
      pub ! ActorPublisherMessage.Request(1)

      val proxy1 = pub.underlyingActor.context.child("test1")
        .getOrElse(throw new Exception(s"test1 not found in: ${pub.underlyingActor.context.children}"))
      val proxy2 = pub.underlyingActor.context.child("test2")
        .getOrElse(throw new Exception(s"test2 not found in: ${pub.underlyingActor.context.children}"))

      proxy1 ! StreamStarted
      proxy2 ! StreamStarted
      expectStreamStarted() // 1 only as concat should not set publisher to active until first source is completed

      pub ! ActorPublisherMessage.Request(1)
      val meta2 = TypeMetadata(Vector("test" → JsNumber(2)))
      proxy2 ! meta2
      expectNoMsg(100.millis)

      val meta = TypeMetadata(Vector("test" → JsNumber(1)))
      proxy1 ! meta
      expectTypeMetadata() shouldBe meta

      pub ! ActorPublisherMessage.Request(1)
      // should not forward any other progress state other than Running
      proxy1 ! QueryProgress(QueryProgress.Started, 0, None, None)
      expectNoMsg(100.millis)

      proxy1 ! QueryProgress(QueryProgress.Running, 1, Some(10), Some("blah"))
      expectMsgType[QueryProgress] shouldBe QueryProgress(QueryProgress.Running, 5.0d, Some(100d), Some("%"))

      val out = OutputChunk(Vector(1))
      proxy1 ! out
      pub ! ActorPublisherMessage.Request(1)
      expectMsgType[OutputChunk] shouldBe out

      val out2 = OutputChunk(Vector(2))
      pub ! ActorPublisherMessage.Request(2)
      proxy1 ! out2
      expectMsgType[OutputChunk] shouldBe out2

      val out3 = OutputChunk(Vector(0))
      proxy1 ! out3
      expectMsgType[OutputChunk] shouldBe out3

      pub ! ActorPublisherMessage.Request(100)
      proxy1 ! StreamCompleted("", None)

      // 2nd source, buffered message
      expectTypeMetadata() shouldBe meta2

      val out4 = OutputChunk(Vector(10))
      proxy2 ! out4
      expectMsgType[OutputChunk] shouldBe out4

      proxy2 ! QueryProgress(QueryProgress.Running, 10, Some(20), Some("blah"))
      expectMsgType[QueryProgress] shouldBe QueryProgress(QueryProgress.Running, 50.0d, Some(100d), Some("%"))

      proxy2 ! StreamCompleted("", None)
      expectDone(pub)
    }

    "merge two simple queries' streams" in {
      val query1 = Query.apply("10", mockConfig, None)
      val query2 = Query.apply("10", mockConfig, None)
      val pub = newPublisher(root, ComposedQuery(query1, 0, Some("test1")) ::
        ComposedQuery(query2, 0, Some("test2")) :: Nil, Composer.MergeStrategy)

      // force instantiate underlying publishers
      pub ! ActorPublisherMessage.Request(1)

      val proxy1 = pub.underlyingActor.context.child("test1")
        .getOrElse(throw new Exception(s"test1 not found in: ${pub.underlyingActor.context.children}"))
      val proxy2 = pub.underlyingActor.context.child("test2")
        .getOrElse(throw new Exception(s"test2 not found in: ${pub.underlyingActor.context.children}"))

      proxy1 ! StreamStarted
      proxy2 ! StreamStarted
      expectStreamStarted() // 1 only as concat should not set publisher to active until first source is completed

      pub ! ActorPublisherMessage.Request(2)
      val meta2 = TypeMetadata(Vector("test" → JsNumber(2)))
      proxy2 ! meta2
      expectTypeMetadata() shouldBe meta2

      val meta = TypeMetadata(Vector("test" → JsNumber(1)))
      proxy1 ! meta
      expectTypeMetadata() shouldBe meta

      pub ! ActorPublisherMessage.Request(3)
      // should not forward any other progress state other than Running
      proxy1 ! QueryProgress(QueryProgress.Started, 0, None, None)
      expectNoMsg(100.millis)

      proxy1 ! QueryProgress(QueryProgress.Running, 1, Some(10), Some("blah"))
      expectMsgType[QueryProgress] shouldBe QueryProgress(QueryProgress.Running, 5.0d, Some(100d), Some("%"))

      proxy2 ! QueryProgress(QueryProgress.Running, 10, Some(20), Some("somethingElse"))
      expectMsgType[QueryProgress] shouldBe QueryProgress(QueryProgress.Running, 25.0d, Some(100d), Some("%"))

      proxy1 ! QueryProgress(QueryProgress.Running, 19, Some(20), Some("blah"))
      expectMsgType[QueryProgress] shouldBe QueryProgress(QueryProgress.Running, 47.5d, Some(100d), Some("%"))

      val out = OutputChunk(Vector(1))
      proxy1 ! out
      pub ! ActorPublisherMessage.Request(100)
      expectMsgType[OutputChunk] shouldBe out

      val out4 = OutputChunk(Vector(10))
      proxy2 ! out4
      expectMsgType[OutputChunk] shouldBe out4

      val out2 = OutputChunk(Vector(2))
      proxy1 ! out2
      expectMsgType[OutputChunk] shouldBe out2

      val out3 = OutputChunk(Vector(0))
      proxy1 ! out3
      expectMsgType[OutputChunk] shouldBe out3

      proxy1 ! StreamCompleted("", None)

      val out5 = OutputChunk(Vector(11))
      proxy2 ! out5
      expectMsgType[OutputChunk] shouldBe out5

      proxy2 ! StreamCompleted("", None)

      expectDone(pub)
    }

    "just run a single query" in {
      {
        val pub = newPublisher(root, ComposedQuery(syntheticQuery, 0) :: Nil, Composer.ConcatStrategy)
        pub ! ActorPublisherMessage.Request(200)
        val msg = receiveN(111)
        expectDone(pub)
      }

      {
        val pub = newPublisher(root, ComposedQuery(syntheticQuery, 0) :: Nil, Composer.MergeStrategy)
        pub ! ActorPublisherMessage.Request(200)
        val msg = receiveN(111)
        expectDone(pub)
      }
    }

    "provide incremental metadata when different streams have different schemas" in {
      val query1 = Query.apply("10", mockConfig, None)
      val query2 = Query.apply("10", mockConfig, None)
      val pub = newPublisher(root, ComposedQuery(query1, 0, Some("test1")) ::
        ComposedQuery(query2, 0, Some("test2")) :: Nil, Composer.MergeStrategy)

      pub ! ActorPublisherMessage.Request(100)
      expectStreamStarted()

      val proxy1 = pub.underlyingActor.context.child("test1").get
      val proxy2 = pub.underlyingActor.context.child("test2").get

      val meta2 = TypeMetadata(Vector("test" → JsNumber(2)))
      proxy2 ! meta2
      expectTypeMetadata() shouldBe meta2

      val meta = TypeMetadata(Vector("test" → JsString("2")))
      proxy1 ! meta
      expectTypeMetadata() shouldBe meta

      // same meta, so it should not update
      proxy1 ! meta
      expectNoMsg()

      // type change
      proxy2 ! meta2
      expectTypeMetadata() shouldBe meta2

      // new field
      val meta3 = TypeMetadata(Vector("test2" → JsNumber(2)))
      proxy2 ! meta3
      expectTypeMetadata() shouldBe TypeMetadata(Vector("test" → JsNumber(2), "test2" → JsNumber(2)))

      // type change
      proxy1 ! TypeMetadata(Vector("test" → JsNumber(2), "test2" → JsString("2")))
      expectTypeMetadata() shouldBe TypeMetadata(Vector("test" → JsNumber(2), "test2" → JsString("2")))

      // subset
      proxy1 ! TypeMetadata(Vector("test" → JsNumber(2)))
      expectNoMsg()

      pub ! PoisonPill
    }

    // TODO
    "provide stream priority re-ordering" in {
    }
  }
}
