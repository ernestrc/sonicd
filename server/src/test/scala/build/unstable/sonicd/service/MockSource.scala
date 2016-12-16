package build.unstable.sonicd.service

import akka.actor.{Actor, ActorContext, Props}
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.Request
import build.unstable.sonic.model._
import build.unstable.sonicd.SonicdLogging

import scala.collection.mutable

class MockSource(query: Query, actorContext: ActorContext, context: RequestContext)
  extends DataSource(query, actorContext, context) {

  override def publisher: Props = Props(classOf[ProxyPublisher])
}

class ProxyPublisher extends Actor with ActorPublisher[SonicMessage] with SonicdLogging {

  val buffer = mutable.Queue.empty[SonicMessage]

  override def unhandled(message: Any): Unit = {
    log.warning(">>>>>>>>>>>>>>>> unhandled message for mock publisher: {}", message)
  }

  override def receive: Receive = {
    case c: StreamCompleted ⇒
      if (isActive && totalDemand > 0) {
        onNext(c)
        onCompleteThenStop()
      } else context.become({
        case Request(_) ⇒
          onNext(c)
          onCompleteThenStop()
      })
    case m: SonicMessage ⇒
      if (isActive && totalDemand > 0) onNext(m)
      else buffer.enqueue(m)
    case r: Request ⇒
      while (isActive && totalDemand > 0 && buffer.nonEmpty) {
        onNext(buffer.dequeue())
      }
  }
}
