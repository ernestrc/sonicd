package build.unstable.sonicd.api

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import akka.util.Timeout
import build.unstable.sonicd.BuildInfo

class MonitoringEndpoint(responseTimeout: Timeout)(implicit val mat: Materializer, system: ActorSystem) {

  implicit val timeout: Timeout = responseTimeout

  implicit val logger: LoggingAdapter = system.log

  val route: Route = path("version") {
    get {
      complete {
        s"${BuildInfo.version} (${BuildInfo.commit} ${BuildInfo.builtAt})"
      }
    }
  }
}
