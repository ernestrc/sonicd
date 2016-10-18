package build.unstable.sonicd.api

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import akka.util.Timeout
import build.unstable.sonic.server.http.EndpointUtils
import build.unstable.sonicd.BuildInfo

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
