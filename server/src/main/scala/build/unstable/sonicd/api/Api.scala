package build.unstable.sonicd.api

import akka.actor.{ActorRef, Props}
import akka.http.scaladsl.model.MediaTypes._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.http.scaladsl.settings.RoutingSettings
import akka.util.CompactByteString
import build.unstable.sonicd.SonicdConfig
import build.unstable.sonicd.api.auth.AuthEndpoint
import build.unstable.sonicd.api.endpoint.{QueryEndpoint, MonitoringEndpoint}
import build.unstable.sonicd.model.{SonicdLogging, JsonProtocol, Receipt}
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

  def completeWithMessage(msg: String, rejection: Rejection) = {
    complete(HttpResponse(BadRequest,
      entity = HttpEntity.Strict(ContentType(`application/json`),
        CompactByteString(JsonProtocol.receiptJsonFormat.write(
          Receipt.error(new Exception(s"${rejection.toString}"), msg)).toString))))
  }

  implicit val rejectionHandler: RejectionHandler.Builder = RejectionHandler.newBuilder().handle {
    case rej@MalformedRequestContentRejection(msg, _) ⇒
      warning(log, "malformed request: {}", rej)
      completeWithMessage("there was a problem when unmarshalling payload: " + msg, rej)
    case rej: Rejection ⇒
      warning(log, "rejected: {}", rej)
      completeWithMessage("rejected", rej)
  }

  implicit val receiptExceptionHandler = ExceptionHandler {
    case e: Exception ⇒
      log.error("unexpected error", e)
      complete(HttpResponse(InternalServerError,
        entity = HttpEntity.Strict(ContentType(`application/json`),
          CompactByteString(JsonProtocol.receiptJsonFormat.write(
            Receipt.error(e, "Oops! There was an unexpected Error")).toString()))))
  }

  val queryEndpoint = new QueryEndpoint(controllerService, authenticationService,
    SonicdConfig.ENDPOINT_TIMEOUT, SonicdConfig.ACTOR_TIMEOUT)

  val authEndpoint = new AuthEndpoint(authenticationService, SonicdConfig.ENDPOINT_TIMEOUT)

  val monitoringEndpoint = new MonitoringEndpoint(SonicdConfig.ENDPOINT_TIMEOUT, controllerService)

  val httpHandler = logRequest("sonic") {
    Route.seal(pathPrefix(SonicdConfig.API_VERSION) {
      handleExceptions(receiptExceptionHandler) {
        queryEndpoint.route ~
          monitoringEndpoint.route ~
          authEndpoint.route
      }
    })(RoutingSettings.default,
      rejectionHandler = rejectionHandler.result(),
      exceptionHandler = receiptExceptionHandler)
  }
}
