package build.unstable.sonicd.system

import akka.actor.{Actor, ActorSystem}
import build.unstable.tylog.{TypedLogging, TypedLoggingAdapter}

trait SonicdActorLogging extends SonicdLogging {
  this: Actor ⇒

  def system: ActorSystem = context.system
}

trait SonicdLogging extends TypedLogging[SonicdLoggingAdapter] {
  this: {def system: ActorSystem} ⇒

  override def log: SonicdLoggingAdapter = SonicdLoggingAdapter(this.getClass, system)
}

case class SonicdLoggingAdapter(clazz: Class[_], system: ActorSystem)
  extends TypedLoggingAdapter(clazz, system) {

  def template(arg: Any*): String = format("{}, {}", arg)

  type TraceId = String
}
