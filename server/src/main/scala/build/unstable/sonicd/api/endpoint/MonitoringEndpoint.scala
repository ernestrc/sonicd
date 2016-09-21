package build.unstable.sonicd.api.endpoint

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import akka.util.Timeout
import build.unstable.sonicd.BuildInfo
import build.unstable.sonicd.api.EndpointUtils
import org.slf4j.event.Level

class MonitoringEndpoint(responseTimeout: Timeout, controller: ActorRef)
                        (implicit val mat: ActorMaterializer, system: ActorSystem) extends EndpointUtils {

  implicit val t: Timeout = responseTimeout

  val route: Route = path("version") {
    get {
      complete {
        s"${BuildInfo.version} (${BuildInfo.commit} ${BuildInfo.builtAt})"
      }
    }
  }
}
