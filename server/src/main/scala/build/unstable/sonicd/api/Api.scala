package build.unstable.sonicd.api

import akka.actor.{ActorRef, Props}
import akka.http.scaladsl.model.MediaTypes._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.http.scaladsl.settings.RoutingSettings
import akka.util.CompactByteString
import build.unstable.sonicd.SonicConfig
import build.unstable.sonicd.model.{JsonProtocol, Receipt}
import build.unstable.sonicd.system.{TcpSupervisor, Service, System}

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
  this: System with Service ⇒

  def completeWithMessage(msg: String, rejection: Rejection) = {
    complete(HttpResponse(BadRequest,
      entity = HttpEntity.Strict(ContentType(`application/json`),
        CompactByteString(JsonProtocol.receiptJsonFormat.write(
          Receipt.error(new Exception(s"${rejection.toString}"), msg)).toString))))
  }

  implicit val rejectionHandler: RejectionHandler.Builder = RejectionHandler.newBuilder().handle {
    case rej@MalformedRequestContentRejection(msg, _) ⇒ completeWithMessage("There was a problem when unmarshalling payload: " + msg, rej)
    case rej: Rejection ⇒ completeWithMessage("Oops!", rej)
  }

  implicit val receiptExceptionHandler = ExceptionHandler {
    case e: Exception ⇒
      system.log.error(e, "Unexpected errorp")
      complete(HttpResponse(InternalServerError,
        entity = HttpEntity.Strict(ContentType(`application/json`),
          CompactByteString(JsonProtocol.receiptJsonFormat.write(
            Receipt.error(e, "Oops! There was an unexpected Error")).toString()))))
  }

  val queryEndpoint = new QueryEndpoint(controllerService, SonicConfig.ENDPOINT_TIMEOUT, SonicConfig.ACTOR_TIMEOUT)

  val monitoringEndpoint = new MonitoringEndpoint(SonicConfig.ENDPOINT_TIMEOUT)

  val httpHandler = logRequest("sonic") {
    Route.seal(pathPrefix(SonicConfig.API_VERSION) {
      handleExceptions(receiptExceptionHandler) {
        queryEndpoint.route ~
          monitoringEndpoint.route
      }
    })(RoutingSettings.default, rejectionHandler = rejectionHandler.result(), exceptionHandler = receiptExceptionHandler)
  }
}
