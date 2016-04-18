package build.unstable.sonicd.system

import akka.actor._
import akka.stream.actor.{ActorPublisher, ActorSubscriber, OneByOneRequestStrategy, RequestStrategy}
import build.unstable.sonicd.model._
import build.unstable.sonicd.system.SonicController.NewQuery
import org.reactivestreams._

import scala.collection.mutable
import scala.concurrent.duration._

class WsHandler(controller: ActorRef) extends ActorPublisher[SonicMessage]
with ActorSubscriber with ActorLogging {

  import akka.stream.actor.ActorPublisherMessage._
  import akka.stream.actor.ActorSubscriberMessage._
  import context.dispatcher

  override protected def requestStrategy: RequestStrategy = OneByOneRequestStrategy

  case object CompletedStream

  def closing(done: Option[DoneWithQueryExecution]): Receive = {
    log.debug("switched to closing behaviour with ev {} and storage {}", done, buffer)
    if (done.isDefined) {
      val msg = done.get
      if (totalDemand > 0) {
        onNext(msg)
        onCompleteThenStop()
      } else {
        buffer.enqueue(msg)
      }
    }
    if (buffer.length == 0) {
      log.debug("called closing and buffer is empty. shutting down..")
      onCompleteThenStop()
    }
    log.debug("switched to closing state but storage is not empty: {}", buffer)
    val recv: Receive = {
      case Cancel | OnComplete ⇒
        onCompleteThenStop()
      case Request(n) =>
        while (isActive && totalDemand > 0 && buffer.length > 0) {
          onNext(buffer.dequeue())
        }
        if (!isActive || buffer.isEmpty) onCompleteThenStop()
    }
    recv orElse commonBehaviour
  }

  val subs = new Subscriber[SonicMessage] {

    override def onError(t: Throwable): Unit = {
      log.error(t, "stream error")
      self ! DoneWithQueryExecution.error(t)
    }

    override def onSubscribe(s: Subscription): Unit = self ! s

    override def onComplete(): Unit = self ! CompletedStream

    override def onNext(t: SonicMessage): Unit = self ! t

  }

  def commonBehaviour: Receive = {

    case OnComplete | Cancel ⇒
      log.debug("client completed/canceled stream")
      context.become(closing(None))

    case CompletedStream ⇒
      log.debug("source completed stream")
      context.become(closing(None))

    case msg@OnError(e) ⇒
      log.error(e, "error in ws stream")
      context.become(closing(Some(DoneWithQueryExecution.error(e))))
  }

  val buffer = mutable.Queue.empty[SonicMessage]

  def materialized(subscription: Subscription): Receive = {
    val recv: Receive = {

      case msg@Request(n) ⇒
        //drain buffer as much as possible given downstream demand
        while (buffer.nonEmpty && totalDemand > 0) {
          val msg = buffer.dequeue()
          onNext(msg)
        }
        if (totalDemand > 0) subscription.request(1)

      case msg: DoneWithQueryExecution ⇒
        context.become(closing(Some(msg)))

      case msg: SonicMessage ⇒
        if (totalDemand > 0) onNext(msg)
        else buffer.enqueue(msg)
        if (totalDemand > 0) subscription.request(1)

      case a@(OnComplete | Cancel) ⇒
        commonBehaviour.apply(a)
        subscription.cancel() //cancel source
    }
    recv orElse commonBehaviour
  }

  def initial: Receive = {

    case s: Subscription ⇒
      s.request(1)
      context.become(materialized(s))

    case handlerProps: Props ⇒
      log.debug("received props {}", handlerProps)
      val handler = context.actorOf(handlerProps)
      val pub = ActorPublisher[SonicMessage](handler)
      pub.subscribe(subs)

    case OnNext(msg) ⇒
      log.debug("client established communication with ws handler")
      try {
        val query = msg.asInstanceOf[Query]
        controller ! NewQuery(query)
      } catch {
        case e: Exception ⇒
          log.error(e, "error while processing query")
          context.become(closing(Some(DoneWithQueryExecution.error(e))))
      }
  }

  override def receive: Receive = initial orElse commonBehaviour
}
