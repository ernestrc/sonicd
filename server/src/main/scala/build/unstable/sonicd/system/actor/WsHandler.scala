package build.unstable.sonicd.system.actor

import java.net.InetAddress
import java.util.UUID

import akka.actor.SupervisorStrategy.Stop
import akka.actor._
import akka.stream.actor.{ActorPublisher, ActorSubscriber, OneByOneRequestStrategy, RequestStrategy}
import build.unstable.sonic.Exceptions.ProtocolException
import build.unstable.sonic.JsonProtocol._
import build.unstable.sonic._
import build.unstable.sonicd.SonicdLogging
import build.unstable.tylog.Variation
import org.reactivestreams._

import scala.util.{Failure, Success}

class WsHandler(controller: ActorRef, authService: ActorRef, clientAddress: Option[InetAddress]) extends ActorPublisher[SonicMessage]
with ActorSubscriber with SonicdLogging {

  import akka.stream.actor.ActorPublisherMessage._
  import akka.stream.actor.ActorSubscriberMessage._

  override protected def requestStrategy: RequestStrategy = OneByOneRequestStrategy

  case object CompletedStream

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

  def awaitingAck: Receive = {
    case Cancel | OnComplete | OnNext(ClientAcknowledge) ⇒ onCompleteThenStop()
  }

  def closing(done: StreamCompleted): Receive = {
    log.debug("switched to closing behaviour with ev {}", done)
    if (isActive && totalDemand > 0) {
      onNext(done)
      awaitingAck
    } else {
      case Cancel | OnComplete | OnNext(ClientAcknowledge) ⇒ onCompleteThenStop()
      case Request(n) =>
        if (isActive) {
          onNext(done)
          context.become(awaitingAck)
        }
    }
  }

  val subs = new Subscriber[SonicMessage] {

    override def onError(t: Throwable): Unit = {
      log.error("publisher called onError of wsHandler", t)
      self ! StreamCompleted.error(traceId, t)
    }

    override def onSubscribe(s: Subscription): Unit = self ! s

    override def onComplete(): Unit = self ! CompletedStream

    override def onNext(t: SonicMessage): Unit = self ! t

  }

  def commonBehaviour: Receive = {

    case Cancel | OnComplete | OnNext(ClientAcknowledge) ⇒
      log.debug("client completed/canceled stream")
      onCompleteThenStop()

    case CompletedStream ⇒
      val msg = "completed stream without done msg"
      log.error(msg)
      context.become(closing(StreamCompleted.error(traceId, new ProtocolException(msg))))

    case msg@OnError(e) ⇒
      error(log, e, "error in ws stream")
      context.become(closing(StreamCompleted.error(traceId, e)))

    case msg: StreamCompleted ⇒
      context.become(closing(msg))

  }

  var pendingToStream: Long = 0L
  implicit var traceId: String = UUID.randomUUID().toString

  def requestTil(implicit s: Subscription): Unit = {
    //make sure that requested is never > than totalDemand
    while (pendingToStream < totalDemand) {
      s.request(1)
      pendingToStream += 1L
    }
  }

  def materialized(subscription: Subscription): Receive = {
    val recv: Receive = {

      case msg@Request(n) ⇒ requestTil(subscription)

      case msg: StreamCompleted ⇒ context.become(closing(msg))

      case a@(OnComplete | Cancel | OnNext(ClientAcknowledge)) ⇒
        commonBehaviour.apply(a)
        subscription.cancel() //cancel source

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
    recv orElse commonBehaviour
  }

  def awaitingReply(traceId: String): Receive = {

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
      requestTil(s)
      context.become(materialized(s))

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
      context.become(awaitingReply(withTraceId.traceId.get) orElse commonBehaviour)

    case OnNext(msg) ⇒
      val msg = "first message should be a Query"
      log.error(msg)
      val e = new ProtocolException(msg)
      context.become(closing(StreamCompleted.error(traceId, e)))

  }

  override def receive: Receive = start orElse commonBehaviour
}
