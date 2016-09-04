package build.unstable.sonicd.service

import java.net.InetAddress

import akka.actor._
import akka.io.Tcp
import akka.testkit.{CallingThreadDispatcher, ImplicitSender, TestActorRef, TestKit}
import akka.util.ByteString
import build.unstable.sonic.JsonProtocol._
import build.unstable.sonic._
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

  implicit val ctx: RequestContext = RequestContext("test-trace-id", None)

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
    val w = Tcp.Write(Sonic.lengthPrefixEncode(prog.toBytes), ack)
    tcpHandler ! prog
    expectMsg(w)
    w
  }

  def sendOutputNoAck(tcpHandler: ActorRef, ack: Int): Tcp.Write = {
    val out = OutputChunk(Vector("1"))
    val ack2 = TcpHandler.Ack(2)
    val w = Tcp.Write(Sonic.lengthPrefixEncode(out.toBytes), ack2)
    tcpHandler ! out
    expectMsg(w)
    w
  }

  def sendDoneNoAck(tcpHandler: ActorRef, ack: Int): Tcp.Write = {
    val done = StreamCompleted.success
    val a = TcpHandler.Ack(ack)
    val w = Tcp.Write(Sonic.lengthPrefixEncode(done.toBytes), a)
    tcpHandler ! done
    expectMsg(w)
    w
  }

  def clientAcknowledge(tcpHandler: TestActorRef[TcpHandler]) = {
    val ack = Sonic.lengthPrefixEncode(ClientAcknowledge.toBytes)
    tcpHandler ! Tcp.Received(ack)
    expectMsg(Tcp.ResumeReading)
    expectIdle(tcpHandler)
  }

  def expectIdle(tcpHandler: TestActorRef[TcpHandler]): Unit = {

    //check that all state has been cleaned
    assert(tcpHandler.underlyingActor.storage.isEmpty)
    assert(tcpHandler.underlyingActor.currentOffset == 0)
    assert(tcpHandler.underlyingActor.transferred == 0)
    assert(tcpHandler.underlyingActor.handler == system.deadLetters)
    assert(tcpHandler.underlyingActor.subscription == null)
    assert(tcpHandler.underlyingActor.dataBuffer.isEmpty)
    tcpHandler ! PoisonPill
  }

  def testSyntheticStream(tcpHandler: ActorRef) {
    val writes = (0 until 104).map { i ⇒
      tcpHandler ! TcpHandler.Ack(i + 1)
      expectMsgClass(classOf[Tcp.Write])
    }

    val msgs = writes.map { case w: Tcp.Write ⇒ SonicMessage.fromBytes(w.data.splitAt(4)._2) }

    msgs.head shouldBe an[StreamStarted]
    msgs.tail.head shouldBe an[TypeMetadata]
    val (progress, tail) = msgs.tail.tail.splitAt(100)
    progress.tail.foreach(_.getClass() shouldBe classOf[QueryProgress])
    tail.head shouldBe a[OutputChunk]
    tail.tail.head shouldBe a[StreamCompleted]
  }


  "should send 'done' event dowsntream if publisher props can't be created" in {
    val tcpHandler = newHandler

    expectMsg(Tcp.ResumeReading)
    tcpHandler ! Tcp.Received(queryBytes)

    expectMsgType[NewQuery]

    val done = StreamCompleted.error(new Exception("oops"))

    tcpHandler ! done

    val ack = TcpHandler.Ack(1)
    expectMsg(Tcp.ResumeReading)
    expectMsg(Tcp.Write(Sonic.lengthPrefixEncode(done.toBytes), ack))

    tcpHandler ! ack

    clientAcknowledge(tcpHandler)
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
    //tcpHandler.underlyingActor.subscription.requested shouldBe 2

    //and should be able to process more
    val ack2 = sendOutputNoAck(tcpHandler, 2).ack
    tcpHandler ! ack2

    tcpHandler.underlyingActor.storage.length shouldBe 0
    tcpHandler.underlyingActor.transferred shouldBe 2
    //tcpHandler.underlyingActor.subscription.requested shouldBe 3
  }

  "should acknowledge and request for more if tcp write succeeds" in {
    val tcpHandler = newHandlerOnStreamingState(zombiePubProps)
    val ack = progressFlowNoAck(tcpHandler).ack

    tcpHandler ! ack

    tcpHandler.underlyingActor.storage.length shouldBe 0
    tcpHandler.underlyingActor.transferred shouldBe 1
    //tcpHandler.underlyingActor.subscription.requested shouldBe 2
  }

  "should buffer a second message if received before 1st message ack" in {
    val tcpHandler = newHandlerOnStreamingState(zombiePubProps)
    val ack = progressFlowNoAck(tcpHandler).ack

    val out = OutputChunk(Vector("1"))
    tcpHandler ! out

    expectNoMsg(1.second)

    val ack2 = TcpHandler.Ack(2)
    tcpHandler ! ack
    expectMsg(Tcp.Write(Sonic.lengthPrefixEncode(out.toBytes), ack2))

  }

  "should send all messages for tcp writing until completed is called" in {
    val tcpHandler = newHandlerOnStreamingState(zombiePubProps)

    val ack = progressFlowNoAck(tcpHandler).ack
    tcpHandler ! ack

    val ack2 = sendOutputNoAck(tcpHandler, 2).ack
    tcpHandler ! ack2

    val ack3 = sendDoneNoAck(tcpHandler, 3).ack
    tcpHandler ! ack3

    tcpHandler ! TcpHandler.CompletedStream

    clientAcknowledge(tcpHandler)

  }

  "should materialize stream and propagate writes to connection" in {
    val tcpHandler = newHandlerOnStreamingState(syntheticPubProps)

    testSyntheticStream(tcpHandler)
    clientAcknowledge(tcpHandler)
  }

  "if peer closes before should go back to idle after client acknowledges" in {
    val tcpHandler = newHandlerOnStreamingState(zombiePubProps)

    tcpHandler ! Tcp.PeerClosed

    val ack1 = sendDoneNoAck(tcpHandler, 1).ack
    tcpHandler ! ack1
    tcpHandler ! TcpHandler.CompletedStream

    clientAcknowledge(tcpHandler)
  }

  "if peer closes before it should terminated when connection breaks" in {
    val (controller, connection, tcpHandler2) = newTestCase("_0", zombiePubProps)

    tcpHandler2 ! Tcp.Received(queryBytes)

    tcpHandler2 ! Tcp.PeerClosed

    val done = StreamCompleted.success
    controller.underlyingActor.isMaterialized shouldBe true
    tcpHandler2 ! done

    connection.underlyingActor.messages shouldBe 1

    connection ! PoisonPill
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
    expectMsg(Tcp.ResumeReading)

    tcpHandler ! Tcp.Received(queryBytes)
    receiveN(2)

    val done = StreamCompleted.error(new Exception("BOOM"))
    val ack = TcpHandler.Ack(1)
    val w = Tcp.Write(Sonic.lengthPrefixEncode(done.toBytes), ack)

    tcpHandler ! done
    expectMsg(w)
    tcpHandler ! ack
    tcpHandler.underlyingActor.storage.length shouldBe 0

    clientAcknowledge(tcpHandler)
  }

  "should terminate if connection breaks before sending query" in {

    val (_, connection, tcpHandler) = newTestCase("_2", syntheticPubProps)
    watch(tcpHandler)
    connection ! PoisonPill
    expectTerminated(tcpHandler)

  }

  "in closing state, it should buffer all messages received and send them for writing when possible before terminating" in {
    val tcpHandler = newHandlerOnStreamingState(zombiePubProps)
    val done = StreamCompleted.success

    val ack1 = TcpHandler.Ack(1)
    val w1 = Tcp.Write(Sonic.lengthPrefixEncode(done.toBytes), ack1)

    val ack2 = TcpHandler.Ack(2)
    val w2 = Tcp.Write(Sonic.lengthPrefixEncode(done.toBytes), ack2)

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
  }

  "for terminating it should wait peer closed and stream is completed if connection is not broken" in {
    val tcpHandler = newHandlerOnStreamingState(zombiePubProps)

    val ack = progressFlowNoAck(tcpHandler).ack
    tcpHandler ! ack

    val ack2 = sendOutputNoAck(tcpHandler, 2).ack
    tcpHandler ! ack2

    val ack3 = sendDoneNoAck(tcpHandler, 3).ack
    tcpHandler ! ack3

    tcpHandler ! TcpHandler.CompletedStream

    tcpHandler.underlying.isTerminated shouldBe false
    clientAcknowledge(tcpHandler)
  }

  "log error and pass error downstream if stream is completed without a done event" in {
    val tcpHandler = newHandlerOnStreamingState(zombiePubProps)

    val ack = progressFlowNoAck(tcpHandler).ack
    tcpHandler ! ack

    val ack2 = sendOutputNoAck(tcpHandler, 2).ack
    tcpHandler ! ack2

    tcpHandler ! TcpHandler.CompletedStream
    tcpHandler.underlying.isTerminated shouldBe false

    expectMsgPF() {
      case w: Tcp.Write ⇒ SonicMessage.fromBytes(w.data.splitAt(4)._2) match {
        case d: StreamCompleted ⇒
          d.success shouldBe false
          assert(d.error.nonEmpty)
          d.error.get.getMessage.contains("Protocol") shouldBe true //an[ProtocolException] doesn't match
          tcpHandler ! w.ack
      }
    }

    clientAcknowledge(tcpHandler)
  }

  "doesnt write done until buffer is empty" in {
    val tcpHandler = newHandlerOnStreamingState(zombiePubProps)

    val ack = progressFlowNoAck(tcpHandler).ack
    tcpHandler ! ack

    val ack2 = sendOutputNoAck(tcpHandler, 2).ack

    //send done before ack2
    val ack3 = TcpHandler.Ack(3)
    val done = StreamCompleted.success
    val w = Tcp.Write(Sonic.lengthPrefixEncode(done.toBytes), ack3)
    tcpHandler ! done
    expectNoMsg()

    tcpHandler ! ack2
    expectMsg(w)

    tcpHandler ! ack3
    clientAcknowledge(tcpHandler)
  }

  "handles cancel msg" in {
    import build.unstable.sonicd.model.Fixture._
    val tcpHandler = newHandlerOnStreamingState(zombiePubProps)
    val ack = progressFlowNoAck(tcpHandler).ack

    tcpHandler ! ack

    val w = Tcp.Received(Sonic.lengthPrefixEncode(CancelStream.toBytes))
    tcpHandler ! w

    assert(tcpHandler.underlyingActor.subscription.isCancelled)

    //should write completed to client
    val ack2 = TcpHandler.Ack(2)
    val w2 = Tcp.Write(Sonic.lengthPrefixEncode(StreamCompleted(syntheticQuery.traceId.get, None).toBytes), ack2)
    expectMsg(w2)
    expectMsg(Tcp.ResumeReading)
    tcpHandler ! ack2

    clientAcknowledge(tcpHandler)
  }

  "should buffer and frame incoming query bytes" in {
    val tcpHandler = newHandler

    expectMsg(Tcp.ResumeReading)
    val (qChunk1, qChunk2) = queryBytes.splitAt(10)

    tcpHandler ! Tcp.Received(qChunk1)
    expectMsg(Tcp.ResumeReading)
    tcpHandler ! Tcp.Received(qChunk2)

    receiveN(2)
    tcpHandler ! PoisonPill
  }

  "should pipeline multiple commands" in {
    val tcpHandler = newHandlerOnStreamingState(syntheticPubProps)

    testSyntheticStream(tcpHandler)
    val ack0 = Sonic.lengthPrefixEncode(ClientAcknowledge.toBytes)
    tcpHandler ! Tcp.Received(ack0)
    expectMsg(Tcp.ResumeReading)

    tcpHandler ! Tcp.Received(queryBytes)
    expectMsgType[NewQuery]
    expectMsg(Tcp.ResumeReading)

    tcpHandler ! syntheticPubProps

    testSyntheticStream(tcpHandler)
    val ack = Sonic.lengthPrefixEncode(ClientAcknowledge.toBytes)
    tcpHandler ! Tcp.Received(ack)
    expectMsg(Tcp.ResumeReading)

    //send 2 new queryies
    tcpHandler ! Tcp.Received(queryBytes)
    tcpHandler ! Tcp.Received(queryBytes)

    //1st query
    expectMsgType[NewQuery]
    expectMsg(Tcp.ResumeReading)
    //2nd query framed
    expectMsg(Tcp.ResumeReading)
    tcpHandler ! syntheticPubProps
    testSyntheticStream(tcpHandler)
    val ack2 = Sonic.lengthPrefixEncode(ClientAcknowledge.toBytes)
    tcpHandler ! Tcp.Received(ack2)
    expectMsg(Tcp.ResumeReading)

    //2nd query
    expectMsgType[NewQuery]
    //2nd query framed again, due to stashing first time
    expectMsg(Tcp.ResumeReading)
    tcpHandler ! syntheticPubProps
    testSyntheticStream(tcpHandler)

    tcpHandler ! PoisonPill
  }

  "should handle authenticate cmd" in {
    val tcpHandler = newHandler

    expectMsg(Tcp.ResumeReading)
    val (qChunk1, qChunk2) = Sonic.lengthPrefixEncode(
      Authenticate("test", "1234", None).toBytes).splitAt(10)

    tcpHandler ! Tcp.Received(qChunk1)
    expectMsg(Tcp.ResumeReading)
    tcpHandler ! Tcp.Received(qChunk2)

    val withTraceId = expectMsgType[Authenticate]
    expectMsg(Tcp.ResumeReading)

    assert(withTraceId.traceId.nonEmpty)

    tcpHandler ! Success("token")

    val out = OutputChunk(Vector("token"))
    val ack = TcpHandler.Ack(1)
    expectMsg(Tcp.Write(Sonic.lengthPrefixEncode(out.toBytes), ack))
    tcpHandler ! ack

    val done = StreamCompleted.success(withTraceId.traceId.get)

    val ack2 = TcpHandler.Ack(2)
    expectMsg(Tcp.Write(Sonic.lengthPrefixEncode(done.toBytes), ack2))

    tcpHandler ! ack2

    clientAcknowledge(tcpHandler)
    expectNoMsg(1.second)
  }
}
