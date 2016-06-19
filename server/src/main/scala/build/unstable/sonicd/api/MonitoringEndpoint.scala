package build.unstable.sonicd.api

import akka.actor.ActorSystem
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import akka.util.Timeout
import build.unstable.sonicd.BuildInfo

class MonitoringEndpoint(responseTimeout: Timeout)
                        (implicit val mat: ActorMaterializer, system: ActorSystem) extends RouteLogging {

  val route: Route = path("version") {
    get {
      instrumentRoute(HandleVersion) { traceId ⇒
        complete {
          s"${BuildInfo.version} (${BuildInfo.commit} ${BuildInfo.builtAt})"
        }
      }
    }
  }
}
