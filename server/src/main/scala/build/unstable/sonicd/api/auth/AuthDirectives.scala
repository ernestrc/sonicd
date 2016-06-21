package build.unstable.sonicd.api.auth

import akka.actor.ActorRef
import akka.http.scaladsl.server.Directive.SingleValueModifiers
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive1, Directives, Rejection}
import akka.pattern.ask
import akka.stream.{Materializer, ActorMaterializer}
import akka.util.Timeout
import build.unstable.sonicd.model.JsonProtocol._
import build.unstable.sonicd.model.{Authenticate, JsonProtocol, SonicdLogging}
import build.unstable.sonicd.system.actor.AuthenticationActor
import build.unstable.tylog.Variation
import spray.json.RootJsonFormat

import scala.util.{Failure, Success, Try}

trait AuthDirectives {
  this: SonicdLogging ⇒

  def mat: Materializer

  case class AuthenticationFailed(msg: String) extends Rejection

  case class LoginFailed(e: Throwable) extends Rejection

  implicit val authJsonFormatter: RootJsonFormat[Authenticate] = jsonFormat3(Authenticate.apply)

  def createAuthToken(authService: ActorRef, t: Timeout, traceId: String): Directive1[AuthenticationActor.Token] =
    entity(as[Authenticate]).flatMap { authCmd =>
      onSuccess {
        trace(log, traceId, GenerateToken, Variation.Attempt, "")
        authService.ask(authCmd)(t)
          .mapTo[Try[AuthenticationActor.Token]]
          .andThen {
            case Success(token) ⇒
              trace(log, traceId, GenerateToken, Variation.Success, "created token {}", token)
            case Failure(e) ⇒
              trace(log, traceId, GenerateToken, Variation.Failure(e), "failed to create token")
          }(mat.executionContext)
      }.flatMap {
        case Success(token) ⇒ provide(token)
        case Failure(e) ⇒ reject(LoginFailed(e))
      }
    }

  def tokenFromHeaderAuthentication(authService: ActorRef, t: Timeout, traceId: String): Directive1[ApiUser] =
    headerValueByName("SONICD-AUTH").flatMap { token ⇒
      onSuccess {
        trace(log, traceId, ValidateToken, Variation.Attempt, "sending token {} for validation", token)
        authService.ask(AuthenticationActor.ValidateToken(token, traceId))(t)
          .mapTo[Try[ApiUser]]
          .andThen {
            case Success(res) ⇒
              trace(log, traceId, ValidateToken, Variation.Success, "validated token {}", token)
            case Failure(e) ⇒
              trace(log, traceId, ValidateToken, Variation.Failure(e), "token validation for token {} failed", token)
          }(mat.executionContext)
      }.flatMap {
        case Success(u) ⇒ provide(u)
        case Failure(e) ⇒ reject(AuthenticationFailed(e.getMessage))
      }
    }

}
