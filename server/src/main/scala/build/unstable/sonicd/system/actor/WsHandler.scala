package build.unstable.sonicd.system.actor

import java.net.InetAddress
import java.util.UUID

import akka.actor.SupervisorStrategy.Stop
import akka.actor._
import akka.stream.actor._
import build.unstable.sonic.Exceptions.ProtocolException
import build.unstable.sonic.JsonProtocol._
import build.unstable.sonic._
import build.unstable.sonicd.SonicdLogging
import build.unstable.sonicd.model.StreamSubscription
import build.unstable.tylog.Variation
import org.reactivestreams._

import scala.util.{Failure, Success}

class WsHandler(controller: ActorRef, authService: ActorRef, clientAddress: Option[InetAddress]) extends ActorPublisher[SonicMessage]
with ActorSubscriber with SonicdLogging with Stash {

  import akka.stream.actor.ActorPublisherMessage._
  import akka.stream.actor.ActorSubscriberMessage._

  override protected def requestStrategy: RequestStrategy = OneByOneRequestStrategy

  case object UpstreamCompleted

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case e: Exception ⇒
      log.error("error in publisher", e)
      self ! StreamCompleted.error(traceId, e)
      Stop
  }

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    debug(log, "starting ws handler in path {}", self.path)
  }


  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    debug(log, "stopped ws handler in path '{}'", self.path)
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
  }
  
  def restartInternally(): Unit = {
    setInitState()
    unstashAll()
    context.become(receive)
  }

  val subs = new Subscriber[SonicMessage] {

    override def onError(t: Throwable): Unit = {
      log.error("publisher called onError of wsHandler", t)
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

  setInitState()


  /* BEHAVIOUR */

  def waitingAck:Receive = stashCommands orElse {
    case ActorPublisherMessage.Cancel | ActorSubscriberMessage.OnComplete ⇒ onCompleteThenStop()
    case OnNext(ClientAcknowledge) ⇒ restartInternally()
  }

  def closing(done: StreamCompleted): Receive = stashCommands orElse {
    log.debug("switched to closing behaviour with ev {}", done)
    if (isActive && totalDemand > 0) {
      onNext(done)
      waitingAck
    } else commonBehaviour orElse {
      case OnNext(ClientAcknowledge) ⇒ restartInternally()
      case UpstreamCompleted ⇒ //ignore
      case Request(n) => 
        onNext(done)
        context.become(waitingAck)
      //this can only happen if cancel was sent between props and subscription
      case s: Subscription ⇒ s.cancel()
    }
  }

  def commonBehaviour: Receive = {

    case ActorPublisherMessage.Cancel | ActorSubscriberMessage.OnComplete ⇒
      log.debug("client completed/canceled stream")
      onCompleteThenStop()

    case UpstreamCompleted ⇒
      val msg = "completed stream without done msg"
      log.error(msg)
      context.become(closing(StreamCompleted.error(traceId, new ProtocolException(msg))))

    case msg@OnError(e) ⇒
      error(log, e, "error in ws stream")
      context.become(closing(StreamCompleted.error(traceId, e)))

    case msg: StreamCompleted ⇒
      context.become(closing(msg))

  }

  def stashCommands: Receive = {
    case OnNext(i: SonicCommand) ⇒ stash()
  }

  def materialized: Receive = stashCommands orElse {
    case OnNext(CancelStream) ⇒
      subscription.cancel() //cancel source
      context.become(closing(StreamCompleted.success(traceId)))

    case a@(ActorSubscriberMessage.OnComplete | ActorPublisherMessage.Cancel) ⇒
      subscription.cancel()
      onCompleteThenStop()

    case msg@Request(n) ⇒ requestTil()

    case msg: StreamCompleted ⇒ context.become(closing(msg))

    case msg: SonicMessage ⇒
      try {
        if (isActive) {
          onNext(msg)
          pendingToStream -= 1L
        }
        else warning(log, "dropping message {}: wsHandler is not active", msg)
      } catch {
        case e: Exception ⇒
          error(log, e, "error onNext: pending: {}; demand: {}", pendingToStream, totalDemand)
          context.become(closing(StreamCompleted.error(traceId, e)))
      }
  }

  def waiting(traceId: String): Receive = stashCommands orElse {

    case OnNext(CancelStream) ⇒ context.become(closing(StreamCompleted.success(traceId)))

    case a@(ActorSubscriberMessage.OnComplete | ActorPublisherMessage.Cancel) ⇒ onCompleteThenStop()

    //auth cmd failed
    case Failure(e) ⇒
      trace(log, traceId, GenerateToken, Variation.Failure(e), "")
      context.become(closing(StreamCompleted.error(traceId, e)))

    //auth cmd succeded
    case Success(token: AuthenticationActor.Token) ⇒
      trace(log, traceId, GenerateToken, Variation.Success, "received new token '{}'", token)
      onNext(OutputChunk(Vector(token)))
      context.become(closing(StreamCompleted.success(traceId)))

    case s: Subscription ⇒
      trace(log, traceId, MaterializeSource, Variation.Success, "subscribed")
      subscription = new StreamSubscription(s)
      requestTil()
      context.become(materialized orElse commonBehaviour)

    case handlerProps: Props ⇒
      log.debug("received props {}", handlerProps)
      val handler = context.actorOf(handlerProps)
      val pub = ActorPublisher[SonicMessage](handler)
      pub.subscribe(subs)

    case msg: StreamCompleted ⇒
      trace(log, traceId, MaterializeSource, Variation.Failure(msg.error.get), "error materializing source")
      context.become(closing(msg))

  }

  def start: Receive = {

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
          trace(log, withTraceId.traceId.get, MaterializeSource, Variation.Attempt, "recv query {}", q)

          controller ! SonicController.NewQuery(q, clientAddress)

        case a: Authenticate ⇒
          trace(log, withTraceId.traceId.get, GenerateToken, Variation.Attempt, "recv authenticate cmd {}", a)

          authService ! a
      }
      context.become(waiting(withTraceId.traceId.get) orElse commonBehaviour)

    case OnNext(msg) ⇒
      val msg = "first message should be a SonicCommand"
      log.error(msg)
      val e = new ProtocolException(msg)
      context.become(closing(StreamCompleted.error(traceId, e)))

  }

  override def receive: Receive = start orElse commonBehaviour
}
