package build.unstable.sonicd.service.source

import akka.testkit.ImplicitSender
import build.unstable.sonicd.model.SonicMessage
import org.reactivestreams.{Subscription, Subscriber}

trait ImplicitSubscriber extends Subscriber[SonicMessage] {
  this: ImplicitSender ⇒

  val subs = this

  override def onError(t: Throwable): Unit = self ! t

  override def onSubscribe(s: Subscription): Unit = { }

  override def onComplete(): Unit = self ! "complete"

  override def onNext(t: SonicMessage): Unit = self ! t
}
