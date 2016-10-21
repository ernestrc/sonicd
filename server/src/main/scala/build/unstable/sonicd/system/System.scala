package build.unstable.sonicd.system

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}

/**
 * Interface containing the [[akka.actor.ActorSystem]]
 */
trait System {
  implicit val system: ActorSystem

  implicit val materializer: ActorMaterializer

}

/**
 * It implements ``System`` by instantiating the ActorSystem and registering
 * the JVM termination hook to shutdown the ActorSystem on JVM exit.
 */
trait AkkaSystem extends System {
  implicit val system = ActorSystem("sonic")

  val matSettings: ActorMaterializerSettings = ActorMaterializerSettings(system)

  implicit val materializer: ActorMaterializer = ActorMaterializer(matSettings)

  sys.addShutdownHook(system.terminate())

}
