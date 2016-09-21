package build.unstable.sonicd.api

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.http.scaladsl.settings.RoutingSettings
import build.unstable.sonicd.{SonicdLogging, SonicdConfig}
import build.unstable.sonicd.api.auth.AuthEndpoint
import build.unstable.sonicd.api.endpoint.{MonitoringEndpoint, QueryEndpoint}
import build.unstable.sonicd.system.{Service, System}

trait Api {
  val httpHandler: Route
}

/**
 * The REST API layer. It exposes the REST services, but does not provide any
 * web server interface.
 *
 * Notice that it requires to be mixed in with ``Service`` which provides access
 * to the top-level actors that make up the system.
 */
trait AkkaApi extends Api {
  this: System with Service with SonicdLogging ⇒

  def completeWithMessage(msg: String) = {
    complete(HttpResponse(BadRequest, entity = msg + ": "))
  }

  implicit val rejectionHandler: RejectionHandler.Builder = RejectionHandler.newBuilder().handle {
    case rej@MalformedRequestContentRejection(msg, _) ⇒
      log.warning( "malformed request: {}", rej)
      completeWithMessage("there was a problem when unmarshalling payload: " + msg)
    case rej: Rejection ⇒
      log.warning( "rejected: {}", rej)
      completeWithMessage("rejected")
  }

  val queryEndpoint = new QueryEndpoint(controllerService, authenticationService,
    SonicdConfig.ENDPOINT_TIMEOUT, SonicdConfig.ACTOR_TIMEOUT)

  val authEndpoint = new AuthEndpoint(authenticationService, SonicdConfig.ENDPOINT_TIMEOUT)

  val monitoringEndpoint = new MonitoringEndpoint(SonicdConfig.ENDPOINT_TIMEOUT, controllerService)

  val httpHandler = logRequest("sonic") {
    Route.seal(pathPrefix(SonicdConfig.API_VERSION) {
      queryEndpoint.route ~
        monitoringEndpoint.route ~
        authEndpoint.route
    }
    )(RoutingSettings.default,
      rejectionHandler = rejectionHandler.result())
  }

}
