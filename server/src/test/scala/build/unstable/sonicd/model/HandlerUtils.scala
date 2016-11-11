package build.unstable.sonicd.model

import akka.actor.{Actor, ActorRef, Props}
import akka.stream.actor.{ActorPublisher, ActorPublisherMessage}
import akka.stream.scaladsl.Source
import akka.testkit.{CallingThreadDispatcher, TestKitBase}
import build.unstable.sonic.model.{QueryProgress, StreamCompleted, StreamStarted, TypeMetadata}
import org.reactivestreams.{Publisher, Subscriber}

import scala.collection.mutable

trait HandlerUtils {
  this: TestKitBase ⇒

  def expectQueryProgress(progress: Long,
                          status: QueryProgress.Status,
                          total: Option[Long],
                          units: Option[String]): QueryProgress = {
    val msg = expectMsgAnyClassOf(classOf[QueryProgress])

    assert(msg.status == status, s"status: ${msg.status} was not equal to $status")
    assert(msg.units == units, s"units ${msg.units} was not equal to $units")
    assert(msg.total == total, s"total ${msg.total} not equal to $total")
    assert(msg.progress == progress, s"progress: ${msg.progress} was not equal to $progress")
    msg
  }

  def expectTypeMetadata(): TypeMetadata = {
    expectMsgAnyClassOf(classOf[TypeMetadata])
  }

  def expectStreamStarted(): StreamStarted = {
    expectMsgAnyClassOf(classOf[StreamStarted])
  }

  def expectDone(pub: ActorRef, success: Boolean = true): StreamCompleted = {
    val d = expectMsgType[StreamCompleted]
    if (success) assert(d.success)
    else assert(!d.success)

    expectMsg("complete") //sent by ImplicitSubscriber
    expectTerminated(pub)
    d
  }

  def newProxyPublisher[K]: ActorRef =
    system.actorOf(Props[TestPublisher[K]].withDispatcher(CallingThreadDispatcher.Id))

  def newProxySource[K](publisher: ActorRef): Source[K, _] = Source.fromPublisher(ActorPublisher.apply(publisher))
}

// publisher that proxies messages to subscriber
class TestPublisher[K] extends ActorPublisher[K] with Actor {

  val buffer = mutable.Queue.empty[K]

  override def receive: Receive = {
    case ActorPublisherMessage.Request(_) ⇒
      while (buffer.nonEmpty && totalDemand > 0) {
        onNext(buffer.dequeue())
      }
    case ActorPublisherMessage.Cancel ⇒ //
    case msg if isActive && totalDemand > 0 ⇒ onNext(msg.asInstanceOf[K])
    case msg ⇒ buffer.enqueue(msg.asInstanceOf[K])
  }
}
