package build.unstable.sonic.model

import org.reactivestreams.Subscription

//wrapper around org.reactivestreams.Subscription
class StreamSubscription(val s: Subscription) {
  private var cancelled: Boolean = false

  def request(n: Long) = s.request(n)

  def cancel(): Unit = {
    if (!cancelled) {
      cancelled = true
      s.cancel()
    }
  }

  def isCancelled: Boolean = cancelled
}
