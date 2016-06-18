package build.unstable.sonicd.system.actor

import java.util.UUID

import akka.actor.SupervisorStrategy.Stop
import akka.actor._
import akka.stream.actor.{ActorPublisher, ActorSubscriber, OneByOneRequestStrategy, RequestStrategy}
import build.unstable.sonicd.model.Exceptions.ProtocolException
import build.unstable.sonicd.model._
import build.unstable.tylog.Variation
import org.reactivestreams._

class WsHandler(controller: ActorRef) extends ActorPublisher[SonicMessage]
with ActorSubscriber with SonicdLogging {

  import akka.stream.actor.ActorPublisherMessage._
  import akka.stream.actor.ActorSubscriberMessage._

  override protected def requestStrategy: RequestStrategy = OneByOneRequestStrategy

  case object CompletedStream

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case e: Exception ⇒
      log.error("error in publisher", e)
      self ! DoneWithQueryExecution.error(e)
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

  def closing(done: DoneWithQueryExecution): Receive = {
    log.debug("switched to closing behaviour with ev {}", done)
    if (isActive && totalDemand > 0) {
      onNext(done)
      context.become(awaitingAck)
    }

    {
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
      self ! DoneWithQueryExecution.error(t)
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
      context.become(closing(DoneWithQueryExecution.error(new ProtocolException(msg))))

    case msg@OnError(e) ⇒
      error(log, e, "error in ws stream")
      context.become(closing(DoneWithQueryExecution.error(e)))

    case msg: DoneWithQueryExecution ⇒
      context.become(closing(msg))

  }

  var pendingToStream: Long = 0L

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

      case msg: DoneWithQueryExecution ⇒ context.become(closing(msg))

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
            context.become(closing(DoneWithQueryExecution.error(e)))
        }
    }
    recv orElse commonBehaviour
  }

  def awaitingController(traceId: String): Receive = {

    case s: Subscription ⇒
      trace(log, traceId, MaterializeSource, Variation.Success, "subscribed")
      requestTil(s)
      context.become(materialized(s))

    case handlerProps: Props ⇒
      log.debug("received props {}", handlerProps)
      val handler = context.actorOf(handlerProps)
      val pub = ActorPublisher[SonicMessage](handler)
      pub.subscribe(subs)

    case msg: DoneWithQueryExecution ⇒
      trace(log, traceId, MaterializeSource, Variation.Failure(msg.errors.head), "controller send error")
      context.become(closing(msg))

  }

  def newQuery(withTraceId: Query): Unit = {
    context.become(awaitingController(withTraceId.traceId.get) orElse commonBehaviour)
  }

  def start: Receive = {

    case OnNext(q: Query) ⇒
      val withTraceId = {
        q.traceId match {
          case Some(id) ⇒ q
          case None ⇒ q.copy(trace_id = Some(UUID.randomUUID().toString))
        }
      }
      val msg = "client established communication with ws handler"
      trace(log, withTraceId.traceId.get, MaterializeSource, Variation.Attempt, msg)
      controller ! withTraceId

    case OnNext(msg) ⇒
      val msg = "first message should be a Query"
      log.error(msg)
      val e = new ProtocolException(msg)
      context.become(closing(DoneWithQueryExecution.error(e)))

  }

  override def receive: Receive = start orElse commonBehaviour
}
