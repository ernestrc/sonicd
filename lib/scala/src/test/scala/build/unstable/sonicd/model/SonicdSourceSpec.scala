package build.unstable.sonicd.model

import java.net.InetSocketAddress

import akka.actor.{ActorSystem, _}
import akka.io.Tcp
import akka.stream.actor.{ActorPublisher, ActorPublisherMessage}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.testkit.{CallingThreadDispatcher, ImplicitSender, TestActorRef, TestKit}
import build.unstable.sonic.SonicSupervisor.NewPublisher
import build.unstable.sonic._
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import spray.json._
import JsonProtocol._

import scala.concurrent.duration._

class SonicdSourceSpec(_system: ActorSystem)
  extends TestKit(_system) with WordSpecLike
    with Matchers with BeforeAndAfterAll with ImplicitSender
    with ImplicitSubscriber with HandlerUtils {

  override protected def afterAll(): Unit = {
    materializer.shutdown()
    TestKit.shutdownActorSystem(system)
  }

  def this() = this(ActorSystem("PrestoSourceSpec"))

  val traceId = "test-trace-id"
  implicit val ctx: RequestContext = RequestContext(traceId, None)

  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system))

  val tcpError = "TCPBOOM"

  class TcpException extends Exception(tcpError)

  val mockConfig =
    s"""
       | {
       |  "port" : 8080,
       |  "host" : "sonicd.unstable.build",
       |  "class" : "SonicSource"
       | }
    """.stripMargin.parseJson.asJsObject

  val query1 = """100"""
  val sonicQuery1 = new Query(Some(1L), Some(traceId), None, query1, mockConfig)

  val controller: TestActorRef[TestController] =
    TestActorRef(Props(classOf[TestController], self).
      withDispatcher(CallingThreadDispatcher.Id))

  val testAddr = new InetSocketAddress("0.0.0.0", 3030)

  def newPublisher(context: RequestContext = ctx,
                   dispatcher: String = CallingThreadDispatcher.Id): TestActorRef[SonicPublisher] = {
    val src = new SonicSource(self, sonicQuery1, controller.underlyingActor.context, context)
    val ref = TestActorRef[SonicPublisher](src.handlerProps.withDispatcher(dispatcher))
    ActorPublisher(ref).subscribe(subs)
    watch(ref)
    ref
  }

  "SonicSource" should {
    "run a simple query" in {
      val pub = newPublisher()

      expectMsg(NewPublisher(ctx))

      pub ! ActorPublisherMessage.Request(1)
      pub ! Tcp.Connected(testAddr, testAddr)
      expectMsg(Tcp.Register(pub))

      val bytes = SonicSource.lengthPrefixEncode(sonicQuery1.toBytes)
      val write = Tcp.Write(bytes, SonicPublisher.Ack)
      expectMsg(write)

      //fail 1
      pub ! Tcp.CommandFailed(write)
      expectMsg(Tcp.ResumeWriting)
      pub ! Tcp.WritingResumed
      expectMsg(write)

      //fail 2
      pub ! Tcp.CommandFailed(write)
      expectMsg(Tcp.ResumeWriting)
      pub ! Tcp.WritingResumed
      expectMsg(write)

      //write succeeds
      pub ! SonicPublisher.Ack
      expectMsg(Tcp.ResumeReading)

      expectMsg(StreamStarted(sonicQuery1.traceId.get))

      pub ! ActorPublisherMessage.Request(1)
      expectNoMsg(200.millis)

      //test that framing/buffering is working correctly
      val prog = QueryProgress(QueryProgress.Started, 0, None, None)
      val (first, second) = SonicSource.lengthPrefixEncode(prog.toBytes).splitAt(10)

      pub ! Tcp.Received(first)
      expectMsg(Tcp.ResumeReading)
      expectNoMsg(200.millis)

      pub ! Tcp.Received(second)
      expectMsg(prog)
      expectMsg(Tcp.ResumeReading)

      // test that it respects stream back pressure
      val out = OutputChunk(Vector.empty[String])
      val b = SonicSource.lengthPrefixEncode(out.toBytes)

      pub ! Tcp.Received(b)
      expectMsg(Tcp.ResumeReading)
      expectNoMsg(200.millis)

      pub ! ActorPublisherMessage.Request(1)
      expectMsg(out)

      //test that it closes on done
      val done = DoneWithQueryExecution.success
      val b2 = SonicSource.lengthPrefixEncode(done.toBytes)
      pub ! Tcp.Received(b2)

      val b3 = SonicSource.lengthPrefixEncode(ClientAcknowledge.toBytes)
      val write2 = Tcp.Write(b3, SonicPublisher.Ack)
      expectMsg(write2)
      pub ! SonicPublisher.Ack
      expectMsg(Tcp.ResumeReading)

      pub ! ActorPublisherMessage.Request(1)
      expectMsg(done)

      //stream completed
      expectMsg("complete")
      expectTerminated(pub)
    }

    "bubble up exceptions correctly if upstream connection stage fails" in {
      val pub = newPublisher()

      expectMsg(NewPublisher(ctx))

      pub ! ActorPublisherMessage.Request(1)
      val failed = Tcp.CommandFailed(Tcp.Connect(new InetSocketAddress("", 8080)))
      pub ! failed

      expectMsg(StreamStarted(ctx.traceId))

      pub ! ActorPublisherMessage.Request(1)
      expectMsgType[DoneWithQueryExecution]

      expectMsg("complete")
      expectTerminated(pub)
    }
  }
}

//override supervisor
class SonicSource(implicitSender: ActorRef, query: Query, actorContext: ActorContext, context: RequestContext)
  extends build.unstable.sonic.SonicSource(query, actorContext, context) {

  override lazy val handlerProps: Props = Props(classOf[SonicPublisher], implicitSender, query, context)
}
