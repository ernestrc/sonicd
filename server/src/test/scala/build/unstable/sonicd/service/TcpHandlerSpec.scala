package build.unstable.sonicd.service

import java.net.InetAddress

import akka.actor._
import akka.io.Tcp
import akka.testkit.{CallingThreadDispatcher, ImplicitSender, TestActorRef, TestKit}
import akka.util.ByteString
import build.unstable.sonicd.model.JsonProtocol._
import build.unstable.sonicd.model._
import build.unstable.sonicd.source.SyntheticPublisher
import build.unstable.sonicd.system.actor.SonicController.NewQuery
import build.unstable.sonicd.system.actor.TcpHandler
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.util.Success

class MockController(msg: Any) extends Actor {

  var isTerminated = false
  var isMaterialized = false

  override def receive: Receive = {
    case Terminated(ref) ⇒
      isTerminated = true
    case query: NewQuery ⇒
      isMaterialized = true
      sender() ! msg
      context watch sender()
  }
}


class MockConnection extends Actor {
  var messages = 0
  var bytes = ByteString.empty

  override def receive: Actor.Receive = {
    case Tcp.ResumeReading ⇒ //sure
    case w: Tcp.Write ⇒
      sender() ! w.ack
      messages += 1
      bytes ++= w.data
  }
}

class TcpHandlerSpec(_system: ActorSystem) extends TestKit(_system)
with WordSpecLike with Matchers with BeforeAndAfterAll with ImplicitSender {

  import Fixture._

  def this() = this(ActorSystem("TcpHandlerSpec"))

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  def newTestCase(name: String, replyMsg: Any): (TestActorRef[MockController], TestActorRef[MockConnection], TestActorRef[TcpHandler]) = {
    val controller = TestActorRef[MockController](Props(classOf[MockController], replyMsg), "controller" + name)

    val connection =
      TestActorRef[MockConnection](Props[MockConnection], "connection" + name)

    val tcpHandler =
      TestActorRef[TcpHandler](Props(classOf[TcpHandler],
      controller, self, connection, InetAddress.getLocalHost), "tcpHandler" + name)

    (controller, connection, tcpHandler)
  }

  def newHandler: TestActorRef[TcpHandler] = {
    TestActorRef[TcpHandler](
      Props(classOf[TcpHandler], self, self, self, InetAddress.getLocalHost)
      .withDispatcher(CallingThreadDispatcher.Id))
  }

  def newHandlerOnStreamingState(props: Props): TestActorRef[TcpHandler] = {
    val tcpHandler = newHandler

    expectMsg(Tcp.ResumeReading)

    tcpHandler ! Tcp.Received(queryBytes)
    val q = expectMsgType[NewQuery]
    expectMsg(Tcp.ResumeReading)

    //make sure that traceId is injected
    assert(q.query.traceId.nonEmpty)

    tcpHandler ! props
    tcpHandler
  }

  def progressFlowNoAck(tcpHandler: ActorRef): Tcp.Write = {
    val prog = QueryProgress(QueryProgress.Started, 1, Some(100), None)
    val ack = TcpHandler.Ack(1)
    val w = Tcp.Write(SonicdSource.lengthPrefixEncode(prog.toBytes), ack)
    tcpHandler ! prog
    expectMsg(w)
    w
  }

  def sendOutputNoAck(tcpHandler: ActorRef, ack: Int): Tcp.Write = {
    val out = OutputChunk(Vector("1"))
    val ack2 = TcpHandler.Ack(2)
    val w = Tcp.Write(SonicdSource.lengthPrefixEncode(out.toBytes), ack2)
    tcpHandler ! out
    expectMsg(w)
    w
  }

  def sendDoneNoAck(tcpHandler: ActorRef, ack: Int): Tcp.Write = {
    val done = DoneWithQueryExecution.success
    val a = TcpHandler.Ack(ack)
    val w = Tcp.Write(SonicdSource.lengthPrefixEncode(done.toBytes), a)
    tcpHandler ! done
    expectMsg(w)
    w
  }

  def clientAcknowledge(tcpHandler: ActorRef) = {
    val ack = SonicdSource.lengthPrefixEncode(ClientAcknowledge.toBytes)
    tcpHandler ! Tcp.Received(ack)
    expectMsg(Tcp.ConfirmedClose)
    expectMsg(Tcp.ResumeReading)
    tcpHandler ! Tcp.ConfirmedClosed
  }

  "should buffer and frame incoming query bytes" in {
    val tcpHandler = newHandler
    watch(tcpHandler)

    expectMsg(Tcp.ResumeReading)
    val (qChunk1, qChunk2) = queryBytes.splitAt(10)

    tcpHandler ! Tcp.Received(qChunk1)
    expectMsg(Tcp.ResumeReading)
    tcpHandler ! Tcp.Received(qChunk2)

    receiveN(2)
    clientAcknowledge(tcpHandler)

    expectTerminated(tcpHandler)
  }

  "should handle authenticate cmd" in {
    val tcpHandler = newHandler
    watch(tcpHandler)

    expectMsg(Tcp.ResumeReading)
    val (qChunk1, qChunk2) = SonicdSource.lengthPrefixEncode(
      Authenticate("test", "1234", None).toBytes).splitAt(10)

    tcpHandler ! Tcp.Received(qChunk1)
    expectMsg(Tcp.ResumeReading)
    tcpHandler ! Tcp.Received(qChunk2)

    val withTraceId = expectMsgType[Authenticate]
    expectMsg(Tcp.ResumeReading)

    assert(withTraceId.traceId.nonEmpty)

    tcpHandler ! Success("token")

    val out = OutputChunk(Vector("token"))
    expectMsg(Tcp.Write(SonicdSource.lengthPrefixEncode(out.toBytes), TcpHandler.Ack(1)))
    clientAcknowledge(tcpHandler)

    expectTerminated(tcpHandler)
  }

  "should send 'done' event dowsntream if publisher props can't be created" in {
    val tcpHandler = newHandler
    watch(tcpHandler)

    expectMsg(Tcp.ResumeReading)
    tcpHandler ! Tcp.Received(queryBytes)

    expectMsgType[NewQuery]

    val done = DoneWithQueryExecution.error(new Exception("oops"))

    tcpHandler ! done

    val ack = TcpHandler.Ack(1)
    expectMsg(Tcp.ResumeReading)
    expectMsg(Tcp.Write(SonicdSource.lengthPrefixEncode(done.toBytes), ack))

    tcpHandler ! ack

    clientAcknowledge(tcpHandler)
    expectTerminated(tcpHandler)
  }

  "should retry writing if tcp write fails" in {
    val tcpHandler = newHandlerOnStreamingState(zombiePubProps)
    val write = progressFlowNoAck(tcpHandler)

    tcpHandler ! Tcp.CommandFailed(write)
    expectMsg(Tcp.ResumeWriting)

    tcpHandler ! Tcp.WritingResumed

    expectMsg(write)

    tcpHandler ! write.ack

    tcpHandler.underlyingActor.storage.length shouldBe 0
    tcpHandler.underlyingActor.transferred shouldBe 1
    tcpHandler.underlyingActor.subscription.requested shouldBe 2

    //and should be able to process more
    val ack2 = sendOutputNoAck(tcpHandler, 2).ack
    tcpHandler ! ack2

    tcpHandler.underlyingActor.storage.length shouldBe 0
    tcpHandler.underlyingActor.transferred shouldBe 2
    tcpHandler.underlyingActor.subscription.requested shouldBe 3
  }

  "should acknowledge and request for more if tcp write succeeds" in {
    val tcpHandler = newHandlerOnStreamingState(zombiePubProps)
    val ack = progressFlowNoAck(tcpHandler).ack

    tcpHandler ! ack

    tcpHandler.underlyingActor.storage.length shouldBe 0
    tcpHandler.underlyingActor.transferred shouldBe 1
    tcpHandler.underlyingActor.subscription.requested shouldBe 2
  }

  "should buffer a second message if received before 1st message ack" in {
    val tcpHandler = newHandlerOnStreamingState(zombiePubProps)
    val ack = progressFlowNoAck(tcpHandler).ack

    val out = OutputChunk(Vector("1"))
    tcpHandler ! out

    expectNoMsg(1.second)

    val ack2 = TcpHandler.Ack(2)
    tcpHandler ! ack
    expectMsg(Tcp.Write(SonicdSource.lengthPrefixEncode(out.toBytes), ack2))

  }

  "should send all messages for tcp writing until completed is called" in {
    val tcpHandler = newHandlerOnStreamingState(zombiePubProps)
    watch(tcpHandler)

    val ack = progressFlowNoAck(tcpHandler).ack
    tcpHandler ! ack

    val ack2 = sendOutputNoAck(tcpHandler, 2).ack
    tcpHandler ! ack2

    val ack3 = sendDoneNoAck(tcpHandler, 3).ack
    tcpHandler ! ack3

    tcpHandler ! TcpHandler.CompletedStream

    clientAcknowledge(tcpHandler)
    expectTerminated(tcpHandler)

  }

  "should materialize stream and propagate writes to connection" in {
    val tcpHandler = newHandlerOnStreamingState(syntheticPubProps)
    watch(tcpHandler)

    val writes = (0 until 103).map { i ⇒
      tcpHandler ! TcpHandler.Ack(i + 1)
      expectMsgClass(classOf[Tcp.Write])
    }

    val msgs = writes.map { case w: Tcp.Write ⇒ SonicMessage.fromBytes(w.data.splitAt(4)._2) }

    msgs.head shouldBe an[TypeMetadata]
    val (progress, tail) = msgs.tail.splitAt(100)
    progress.tail.foreach(_ shouldBe QueryProgress(QueryProgress.Running, 1, Some(100), Some("%")))
    tail.head shouldBe a[OutputChunk]
    tail.tail.head shouldBe a[DoneWithQueryExecution]

    clientAcknowledge(tcpHandler)

    expectTerminated(tcpHandler)
  }


  "if peer closes before should terminate after client acknowledges or when connection breaks" in {
    //stream completes
    val tcpHandler = newHandlerOnStreamingState(zombiePubProps)
    watch(tcpHandler)

    tcpHandler ! Tcp.PeerClosed

    val ack1 = sendDoneNoAck(tcpHandler, 1).ack
    tcpHandler ! ack1
    tcpHandler ! TcpHandler.CompletedStream

    clientAcknowledge(tcpHandler)
    expectTerminated(tcpHandler)

    //connection breaks
    val (controller, connection, tcpHandler2) = newTestCase("_0", zombiePubProps)
    watch(tcpHandler2)

    tcpHandler2 ! Tcp.Received(queryBytes)

    tcpHandler2 ! Tcp.PeerClosed

    val done = DoneWithQueryExecution.success
    controller.underlyingActor.isMaterialized shouldBe true
    tcpHandler2 ! done

    connection.underlyingActor.messages shouldBe 1

    connection ! PoisonPill
    expectTerminated(tcpHandler2)

  }

  "should terminate if connection breaks after query has been sent" in {

    val (controller, connection, tcpHandler) = newTestCase("_1", syntheticPubProps)
    watch(tcpHandler)

    tcpHandler ! Tcp.Received(queryBytes)

    controller.underlyingActor.isMaterialized shouldBe true

    connection ! PoisonPill
    expectTerminated(tcpHandler)

  }

  "should handle error event when controller fails to instantiate publisher" in {

    val tcpHandler = newHandler
    watch(tcpHandler)
    expectMsg(Tcp.ResumeReading)

    tcpHandler ! Tcp.Received(queryBytes)
    receiveN(2)

    val done = DoneWithQueryExecution.error(new Exception("BOOM"))
    val ack = TcpHandler.Ack(1)
    val w = Tcp.Write(SonicdSource.lengthPrefixEncode(done.toBytes), ack)

    tcpHandler ! done
    expectMsg(w)
    tcpHandler ! ack
    tcpHandler.underlyingActor.storage.length shouldBe 0

    clientAcknowledge(tcpHandler)
    expectTerminated(tcpHandler)

  }

  "should terminate if connection breaks before sending query" in {

    val (_, connection, tcpHandler) = newTestCase("_2", syntheticPubProps)
    watch(tcpHandler)
    connection ! PoisonPill
    expectTerminated(tcpHandler)

  }

  "in closing state, it should buffer all messages received and send them for writing when possible before terminating" in {
    val tcpHandler = newHandlerOnStreamingState(zombiePubProps)
    watch(tcpHandler)
    val done = DoneWithQueryExecution.success

    val ack1 = TcpHandler.Ack(1)
    val w1 = Tcp.Write(SonicdSource.lengthPrefixEncode(done.toBytes), ack1)

    val ack2 = TcpHandler.Ack(2)
    val w2 = Tcp.Write(SonicdSource.lengthPrefixEncode(done.toBytes), ack2)

    tcpHandler ! done
    expectMsg(w1)

    //oopsie doopsie!
    tcpHandler.underlyingActor.buffer(done)

    tcpHandler ! ack1
    tcpHandler.underlying.isTerminated shouldBe false

    expectMsg(w2)
    tcpHandler ! ack2
    tcpHandler.underlyingActor.storage.length shouldBe 0

    clientAcknowledge(tcpHandler)
    expectTerminated(tcpHandler)
  }

  "for terminating it should wait peer closed and stream is completed if connection is not broken" in {
    val tcpHandler = newHandlerOnStreamingState(zombiePubProps)
    watch(tcpHandler)

    val ack = progressFlowNoAck(tcpHandler).ack
    tcpHandler ! ack

    val ack2 = sendOutputNoAck(tcpHandler, 2).ack
    tcpHandler ! ack2

    val ack3 = sendDoneNoAck(tcpHandler, 3).ack
    tcpHandler ! ack3

    tcpHandler ! TcpHandler.CompletedStream

    tcpHandler.underlying.isTerminated shouldBe false
    clientAcknowledge(tcpHandler)
    expectTerminated(tcpHandler)
  }

  "log error and pass error downstream if stream is completed without a done event" in {
    val tcpHandler = newHandlerOnStreamingState(zombiePubProps)
    watch(tcpHandler)

    val ack = progressFlowNoAck(tcpHandler).ack
    tcpHandler ! ack

    val ack2 = sendOutputNoAck(tcpHandler, 2).ack
    tcpHandler ! ack2

    tcpHandler ! TcpHandler.CompletedStream
    tcpHandler.underlying.isTerminated shouldBe false

    expectMsgPF() {
      case w: Tcp.Write ⇒ SonicMessage.fromBytes(w.data.splitAt(4)._2) match {
        case d: DoneWithQueryExecution ⇒
          d.success shouldBe false
          assert(d.error.nonEmpty)
          d.error.get.getMessage.contains("Protocol") shouldBe true //an[ProtocolException] doesn't match
          tcpHandler ! w.ack
      }
    }

    clientAcknowledge(tcpHandler)
    expectTerminated(tcpHandler)
  }

  "doesnt write done until buffer is empty" in {
    val tcpHandler = newHandlerOnStreamingState(zombiePubProps)
    watch(tcpHandler)

    val ack = progressFlowNoAck(tcpHandler).ack
    tcpHandler ! ack

    val ack2 = sendOutputNoAck(tcpHandler, 2).ack

    //send done before ack2
    val ack3 = TcpHandler.Ack(3)
    val done = DoneWithQueryExecution.success
    val w = Tcp.Write(SonicdSource.lengthPrefixEncode(done.toBytes), ack3)
    tcpHandler ! done
    expectNoMsg()

    tcpHandler ! ack2
    expectMsg(w)

    tcpHandler ! ack3
    clientAcknowledge(tcpHandler)
    expectTerminated(tcpHandler)
  }
}
