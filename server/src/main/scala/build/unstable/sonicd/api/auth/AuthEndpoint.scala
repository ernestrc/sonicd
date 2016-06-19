package build.unstable.sonicd.api.auth

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import akka.util.Timeout
import build.unstable.sonicd.api.EndpointUtils
import build.unstable.sonicd.api.auth.AuthDirectives._

class AuthEndpoint(authenticationService: ActorRef, timeout: Timeout)
                  (implicit val mat: Materializer, system: ActorSystem)
  extends EndpointUtils {

  implicit val t: Timeout = timeout

  val route: Route =
    path("authenticate") {
      post {
        createAuthToken(authenticationService, timeout) { token ⇒
          complete(token)
        }
      }
    }
}
