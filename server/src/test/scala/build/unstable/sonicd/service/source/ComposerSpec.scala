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
                   interleave: Boolean = true,
                   context: RequestContext = testCtx,
                   dispatcher: String = CallingThreadDispatcher.Id): TestActorRef[ComposerPublisher] = {
    implicit val jsonFormat = Composer.getComposedQueryJsonFormat(placeholder, q, context)
    val mockConfig = JsObject(
      Map(
        "strategy" → strategy.toJson,
        "buffer" → JsNumber(bufferSize),
        "interleave" → JsBoolean(interleave),
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
    val syntheticConfig1 = JsObject(Map(
      "class" → JsString("SyntheticSource"),
      "size" → JsNumber(1),
      "progress-delay" → JsNumber(0),
      "schema" → JsObject(Map(
        "test" → JsNumber(1)
      ))
    ))
    val syntheticConfig2 = JsObject(Map(
      "class" → JsString("SyntheticSource"),
      "size" → JsNumber(1),
      "progress-delay" → JsNumber(0),
      "schema" → JsObject(Map(
        "test" → JsNumber(2)
      ))
    ))

    "concat two simple queries' streams" in {
      val query1 = Query.apply("10", syntheticConfig1, None)
      val query2 = Query.apply("10", syntheticConfig2, None)
      val pub = newPublisher(root, ComposedQuery(query1, 0) :: ComposedQuery(query2, 0) :: Nil, Composer.ConcatStrategy)
      pub ! ActorPublisherMessage.Request(1)
      expectStreamStarted()

      pub ! ActorPublisherMessage.Request(1)
      val meta = expectTypeMetadata()
      meta shouldBe TypeMetadata(Vector("test" → JsNumber(1)))

      // combined progress
      receiveProgress(pub, 99, progress ⇒ progress.total shouldBe Some(100))

      pub ! ActorPublisherMessage.Request(1)
      var out = expectMsgType[OutputChunk]
      out shouldBe OutputChunk(Vector(1))

      receiveProgress(pub, 99, progress ⇒ progress.total shouldBe Some(100))

      pub ! ActorPublisherMessage.Request(1)
      out = expectMsgType[OutputChunk]
      out shouldBe OutputChunk(Vector(2))

      pub ! ActorPublisherMessage.Request(1)
      expectDone(pub)
    }

    // "merge two simple queries' streams" in {
    // val query1 = SonicdQuery(Query.apply("10", syntheticConfig1, None))
    //   val query2 = SonicdQuery(Query.apply("10", syntheticConfig2, None))
    //   val pub = newPublisher(root, ComposedQuery(query1, 0) :: ComposedQuery(query2, 0) :: Nil, Composer.MergeStrategy)
    //   pub ! ActorPublisherMessage.Request(1)
    //   expectStreamStarted()

    //   var n = 2
    //   pub ! ActorPublisherMessage.Request(n)
    //   val meta = receiveN(n).asInstanceOf[Seq[TypeMetadata]]
    //   meta should contain(TypeMetadata(Vector("test" → JsNumber(1))))
    //   meta should contain(TypeMetadata(Vector("test" → JsNumber(2))))

    //   // combined progress
    //   n = 202
    //   receiveProgress(pub, n, progress ⇒ progress.total shouldBe Some(100))

    //   n = 2
    //   pub ! ActorPublisherMessage.Request(n)
    //   val out = receiveN(n).asInstanceOf[Seq[OutputChunk]]
    //   out should contain(OutputChunk(Vector(1)))
    //   out should contain(OutputChunk(Vector(2)))

    //  pub ! ActorPublisherMessage.Request(1)
    //  expectDone(pub)
    //}

    "just run a single query" in {
    }

    "combine progress into one" in {
    }
  }
}
