package build.unstable.sonicd.service

import akka.actor.ActorSystem
import akka.testkit.TestKitBase

trait TestSystem extends TestKitBase with build.unstable.sonicd.system.System {
  override implicit val system: ActorSystem
}
