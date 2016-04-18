package build.unstable.sonicd.system

import akka.actor.ActorSystem

/**
 * Interface containing the [[akka.actor.ActorSystem]]
 */
trait System {
  implicit val system: ActorSystem
}

/**
 * It implements ``System`` by instantiating the ActorSystem and registering
 * the JVM termination hook to shutdown the ActorSystem on JVM exit.
 */
trait AkkaSystem extends System {
  implicit val system = ActorSystem("sonicd")

  sys.addShutdownHook(system.terminate())

}
