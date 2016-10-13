package build.unstable.sonicd.auth

import java.util.UUID

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import akka.util.Timeout
import build.unstable.sonicd.api.EndpointUtils

class AuthEndpoint(authenticationService: ActorRef, timeout: Timeout)
                  (implicit val mat: Materializer, val system: ActorSystem)
  extends EndpointUtils with AuthDirectives {

  implicit val t: Timeout = timeout

  val route: Route =
    path("authenticate") {
      post {
        extractTraceHeader { traceIdMaybe ⇒
          val traceId = traceIdMaybe.getOrElse(UUID.randomUUID().toString)
          createAuthToken(authenticationService, timeout, traceId) { token ⇒
            complete(token)
          }
        }
      }
    }
}
