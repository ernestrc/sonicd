package build.unstable.sonicd.service

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.actor.ActorSubscriberMessage.OnNext
import akka.testkit.{CallingThreadDispatcher, ImplicitSender, TestKit}
import build.unstable.sonicd.model.JsonProtocol._
import build.unstable.sonicd.model._
import build.unstable.sonicd.system.actor.WsHandler
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

class WsHandlerSpec(_system: ActorSystem) extends TestKit(_system)
with WordSpecLike with Matchers with BeforeAndAfterAll with ImplicitSender
with ImplicitSubscriber with ImplicitGuardian {

  import Fixture._
  import build.unstable.sonicd.model.Fixture._

  def this() = this(ActorSystem("WsHandlerSpec"))

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  def newWatchedHandler(): ActorRef = {
    val wsHandler = guardian.underlyingActor.context.actorOf(Props(classOf[WsHandler], self)
      .withDispatcher(CallingThreadDispatcher.Id))

    watch(wsHandler)
    ActorPublisher.apply[SonicMessage](wsHandler).subscribe(subs)
    wsHandler
  }

  def expectProgress(wsHandler: ActorRef): Unit = {
    val prog = QueryProgress(Some(100.0), None)
    wsHandler ! prog
    expectMsg(prog)
  }

  def expectOutput(wsHandler: ActorRef): Unit = {
    val out = OutputChunk(Vector("1"))
    wsHandler ! out
    expectMsg(out)
  }

  def expectDone(wsHandler: ActorRef): Unit = {
    val done = DoneWithQueryExecution(success = true)
    wsHandler ! done
    expectMsg(done)
  }
  
  def clientAcknowledge(wsHandler: ActorRef) = {
    val ack = OnNext(ClientAcknowledge)
    wsHandler ! ack
  }

  def expectComplete(wsHandler: ActorRef) {
    //wsHandler ! OnComplete
    clientAcknowledge(wsHandler)
    expectMsg("complete")
    expectTerminated(wsHandler)
  }

  def newHandlerOnStreamingState(props: Props): ActorRef = {
    val wsHandler = newWatchedHandler()
    wsHandler ! OnNext(syntheticQuery)
    val q = expectMsgType[Query]

    //make sure that traceId is injected
    assert(q.traceId.nonEmpty)

    wsHandler ! props
    wsHandler
  }

  "WsHandler" should {
    "should handle error event when controller fails to instantiate publisher" in {
      val wsHandler = newWatchedHandler()

      wsHandler ! OnNext(syntheticQuery)

      val q = expectMsgType[Query]
      assert(q.traceId.nonEmpty)

      val done = DoneWithQueryExecution.error(new Exception("BOOM"))
      wsHandler ! done
      wsHandler ! Request(1)
      expectMsg(done)
      clientAcknowledge(wsHandler)
      expectMsg("complete")
      expectTerminated(wsHandler)
    }

    "should not call onComplete twice (respect ReactiveStreams rules)" in {
      val wsHandler = guardian.underlying.actor.context.actorOf(Props(classOf[WsHandler], self)
        .withDispatcher(CallingThreadDispatcher.Id))

      watch(wsHandler)
      ActorPublisher.apply[SonicMessage](wsHandler).subscribe(subs)

      wsHandler ! OnNext(syntheticQuery)

      val q = expectMsgType[Query]
      assert(q.traceId.nonEmpty)

      subscription.request(1)
      val done = DoneWithQueryExecution.error(new Exception("BOOM"))
      wsHandler ! done
      expectMsg(done)
      clientAcknowledge(wsHandler)
      expectMsg("complete")
      expectTerminated(wsHandler)
    }

    "should forward all messages until completed is called" in {
      val wsHandler = newHandlerOnStreamingState(zombiePubProps)

      wsHandler ! Request(1)
      expectProgress(wsHandler)

      wsHandler ! Request(1)
      expectOutput(wsHandler)

      wsHandler ! Request(1)
      expectDone(wsHandler)

      expectComplete(wsHandler)

    }

    //todo make sure that authenticate gets injected a traceId

    "should materialize stream and propagates messages to downstream subscriber" in {
      val wsHandler = newHandlerOnStreamingState(syntheticPubProps)

      val msgs = (0 until 103).map { i ⇒
        wsHandler ! Request(1)
        expectMsgClass(classOf[SonicMessage])
      }

      msgs.head shouldBe an[TypeMetadata]
      val (progress, tail) = msgs.tail.splitAt(100)
      progress.tail.foreach(_ shouldBe QueryProgress(Some(1), None))
      tail.head shouldBe a[OutputChunk]
      tail.tail.head shouldBe a[DoneWithQueryExecution]

      expectComplete(wsHandler)

    }
  }
}
