package build.unstable.sonic.server.system

import java.net.InetAddress
import java.util.UUID

import akka.actor.SupervisorStrategy.Stop
import akka.actor._
import akka.stream.actor._
import build.unstable.sonic.Exceptions.ProtocolException
import build.unstable.sonic.JsonProtocol._
import build.unstable.sonic.model._
import build.unstable.sonic.server.ServerLogging
import build.unstable.tylog.Variation
import org.reactivestreams._
import org.slf4j.event.Level

import scala.util.control.NonFatal
import scala.util.{Failure, Success}

class WsHandler(controller: ActorRef, authService: ActorRef, clientAddress: Option[InetAddress]) extends ActorPublisher[SonicMessage]
  with ActorSubscriber with ServerLogging with Stash {

  import akka.stream.actor.ActorPublisherMessage._
  import akka.stream.actor.ActorSubscriberMessage._

  override protected def requestStrategy: RequestStrategy = OneByOneRequestStrategy

  case object UpstreamCompleted

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case e: Exception ⇒
      log.error(e, "error in publisher")
      self ! StreamCompleted.error(traceId, e)
      Stop
  }

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    log.debug("starting ws handler in path {}", self.path)
  }


  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.debug("stopped ws handler in path '{}'", self.path)
  }

  override def unhandled(message: Any): Unit = {
    log.warning("message not handled {}", message)
    super.unhandled(message)
  }


  /* HELPERS */

  def requestTil(): Unit = {
    //make sure that requested is never > than totalDemand
    while (pendingToStream < totalDemand) {
      subscription.request(1)
      pendingToStream += 1L
    }
  }

  def setInitState(): Unit = {
    pendingToStream = 0L
    traceId = UUID.randomUUID().toString
    subscription = null
    waitingFor = null
    handler = null
  }

  def restartInternally(): Unit = {
    setInitState()
    unstashAll()
    context.become(receive)
  }

  val subs = new Subscriber[SonicMessage] {

    override def onError(t: Throwable): Unit = {
      log.error(t, "publisher called onError of wsHandler")
      self ! StreamCompleted.error(traceId, t)
    }

    override def onSubscribe(s: Subscription): Unit = self ! s

    override def onComplete(): Unit = self ! UpstreamCompleted

    override def onNext(t: SonicMessage): Unit = self ! t

  }


  /* STATE */

  var pendingToStream: Long = _
  var traceId: String = _
  var subscription: StreamSubscription = _
  var waitingFor: CallType = _
  var handler: ActorRef = _

  setInitState()


  /* BEHAVIOUR */

  def stashCommands: Receive = {
    case OnNext(i: SonicCommand) ⇒ stash()
  }

  def commonBehaviour: Receive = {

    case UpstreamCompleted ⇒
      val msg = "completed stream without done msg"
      val e = new ProtocolException(msg)
      log.error(e, msg)
      context.become(completing(StreamCompleted.error(traceId, e)))

    case msg@OnError(NonFatal(e)) ⇒
      log.warning("ws stream error {}", e)
      context.become(completing(StreamCompleted.error(traceId, e)))

    case msg@OnError(e) ⇒
      log.error(e, "ws stream fatal error")
      onErrorThenStop(e)

  }

  // 4
  def completing(done: StreamCompleted): Receive = stashCommands orElse {
    log.debug("switched to closing behaviour with ev {}", done)
    val recv: Receive = {
      case ActorPublisherMessage.Cancel | ActorSubscriberMessage.OnComplete ⇒
        onComplete()
        context.stop(self)
      case UpstreamCompleted ⇒ //expected, overrides commonBehaviour
      case OnNext(ClientAcknowledge) ⇒ restartInternally()
      //this can only happen if cancel was sent between props and subscription
      case s: Subscription ⇒ s.cancel()
    }

    if (isActive && totalDemand > 0) {
      onNext(done)
      stashCommands orElse recv orElse commonBehaviour orElse {
        case Request(_) ⇒ //ignore
      }
    } else stashCommands orElse recv orElse commonBehaviour orElse {
      case Request(n) =>
        onNext(done)
        context.become(stashCommands orElse recv orElse commonBehaviour)
    }
  }

  // 3
  def materialized: Receive = stashCommands orElse commonBehaviour orElse {

    case ActorSubscriberMessage.OnComplete | ActorPublisherMessage.Cancel ⇒
      // its polite to cancel first before stopping
      subscription.cancel()
      //will succeed
      onComplete()
      context.stop(self)

    case OnNext(CancelStream) ⇒
      subscription.cancel() //cancel source
      context.become(completing(StreamCompleted.success(traceId)))

    case msg@Request(n) ⇒ requestTil()

    case msg: StreamCompleted ⇒ context.become(completing(msg))

    case msg: SonicMessage ⇒
      try {
        if (isActive) {
          onNext(msg)
          pendingToStream -= 1L
        }
        else log.warning("dropping message {}: wsHandler is not active", msg)
      } catch {
        case e: Exception ⇒
          log.error(e, "error onNext: pending: {}; demand: {}", pendingToStream, totalDemand)
          context.become(completing(StreamCompleted.error(traceId, e)))
      }
  }

  // 2
  def waiting(traceId: String): Receive = stashCommands orElse commonBehaviour orElse {
    case r: Request ⇒ //ignore for now

    case ActorPublisherMessage.Cancel | ActorSubscriberMessage.OnComplete ⇒
      val msg = "client completed/canceled stream while waiting for source materialization"
      val e = new Exception(msg)
      log.tylog(Level.INFO, traceId, waitingFor, Variation.Failure(e), msg)
      // we dont need to worry about cancelling subscription here
      // because terminating this actor will stop the source actor
      onComplete()
      context.stop(self)

    case OnNext(CancelStream) ⇒ context.become(completing(StreamCompleted.success(traceId)))

    //auth cmd failed
    case Failure(e) ⇒
      log.tylog(Level.INFO, traceId, waitingFor, Variation.Failure(e), "")
      context.become(completing(StreamCompleted.error(traceId, e)))

    //auth cmd succeded
    case Success(token: String) ⇒
      log.tylog(Level.INFO, traceId, waitingFor, Variation.Success, "received new token '{}'", token)
      onNext(OutputChunk(Vector(token)))
      context.become(completing(StreamCompleted.success(traceId)))

    case s: Subscription ⇒
      log.tylog(Level.INFO, traceId, waitingFor, Variation.Success, "materialized source and subscribed to it")
      subscription = new StreamSubscription(s)
      requestTil()
      context.become(materialized)

    case handlerProps: Props ⇒
      log.debug("received props {}", handlerProps)
      handler = context.actorOf(handlerProps)
      val pub = ActorPublisher[SonicMessage](handler)
      pub.subscribe(subs)

    case msg: StreamCompleted ⇒
      log.tylog(Level.INFO, traceId, waitingFor, Variation.Failure(msg.error.get), "error materializing source")
      context.become(completing(msg))

  }

  // 1
  def receive: Receive = commonBehaviour orElse {
    case r: Request ⇒ //ignore for now

    case ActorPublisherMessage.Cancel ⇒
      log.debug("client completed stream before sending a command")
      context.stop(self)

    case ActorSubscriberMessage.OnComplete ⇒
      log.debug("client completed stream before sending a command")
      onCompleteThenStop()

    case OnNext(i: SonicCommand) ⇒
      val withTraceId = {
        i.traceId match {
          case Some(id) ⇒
            traceId = id
            i
          case None ⇒ i.setTraceId(traceId)
        }
      }
      withTraceId match {
        case q: Query ⇒
          waitingFor = MaterializeSource
          log.tylog(Level.INFO, withTraceId.traceId.get, waitingFor, Variation.Attempt, "processing query {}", q)

          controller ! NewQuery(q, clientAddress)

        case a: Authenticate ⇒
          waitingFor = GenerateToken
          log.tylog(Level.INFO, withTraceId.traceId.get, waitingFor, Variation.Attempt, "processing authenticate cmd {}", a)

          authService ! a
      }
      context.become(waiting(withTraceId.traceId.get))

    case OnNext(msg) ⇒
      val msg = "first message should be a SonicCommand"
      val e = new ProtocolException(msg)
      log.error(e, msg)
      context.become(completing(StreamCompleted.error(traceId, e)))

  }
}
