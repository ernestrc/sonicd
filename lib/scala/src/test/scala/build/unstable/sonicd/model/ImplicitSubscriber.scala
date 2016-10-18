package build.unstable.sonicd.model

import akka.testkit.ImplicitSender
import build.unstable.sonic.model.SonicMessage
import org.reactivestreams.{Subscriber, Subscription}

trait ImplicitSubscriber extends Subscriber[SonicMessage] {
  this: ImplicitSender ⇒

  val subs = this
  var subscription: Subscription = null

  override def onError(t: Throwable): Unit = self ! t

  override def onSubscribe(s: Subscription): Unit = {
    subscription = s
  }

  override def onComplete(): Unit = self ! "complete"

  override def onNext(t: SonicMessage): Unit = self ! t
}
