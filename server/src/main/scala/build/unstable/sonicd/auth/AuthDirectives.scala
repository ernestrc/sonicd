package build.unstable.sonicd.auth

import akka.actor.ActorRef
import akka.http.scaladsl.server.Directive.SingleValueModifiers
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive1, Rejection}
import akka.pattern.ask
import akka.stream.Materializer
import akka.util.Timeout
import build.unstable.sonic.{ApiUser, Authenticate, JsonProtocol, SonicMessage}
import build.unstable.sonicd.SonicdLogging
import JsonProtocol._
import build.unstable.sonicd.system.actor.AuthenticationActor
import build.unstable.tylog.Variation
import org.slf4j.event.Level
import spray.json.{JsValue, RootJsonFormat}

import scala.util.{Failure, Success, Try}

trait AuthDirectives {
  this: SonicdLogging ⇒

  def mat: Materializer

  case class AuthenticationFailed(msg: String) extends Rejection

  case class LoginFailed(e: Throwable) extends Rejection

  implicit val authJsonFormatter: RootJsonFormat[Authenticate] = new RootJsonFormat[Authenticate] {
    override def write(obj: Authenticate): JsValue = obj.json

    override def read(json: JsValue): Authenticate = SonicMessage.fromJson(json) match {
      case a: Authenticate ⇒ a
      case e ⇒ throw new Exception(s"expected Authenticate message found: $e")
    }
  }

  def createAuthToken(authService: ActorRef, t: Timeout, traceId: String): Directive1[AuthenticationActor.Token] =
    entity(as[Authenticate]).flatMap { authCmd =>
      onSuccess {
        log.tylog(Level.INFO,  traceId, GenerateToken, Variation.Attempt, "")
        authService.ask(authCmd)(t)
          .mapTo[Try[AuthenticationActor.Token]]
          .andThen {
            case Success(token) ⇒
              log.tylog(Level.INFO, traceId, GenerateToken, Variation.Success, "created token {}", token)
            case Failure(e) ⇒
              log.tylog(Level.INFO, traceId, GenerateToken, Variation.Failure(e), "failed to create token")
          }(mat.executionContext)
      }.flatMap {
        case Success(token) ⇒ provide(token)
        case Failure(e) ⇒ reject(LoginFailed(e))
      }
    }

  def tokenFromHeaderAuthentication(authService: ActorRef, t: Timeout, traceId: String): Directive1[ApiUser] =
    headerValueByName("SONICD-AUTH").flatMap { token ⇒
      onSuccess {
        log.tylog(Level.INFO, traceId, AuthenticateUser, Variation.Attempt, "sending token {} for validation", token)
        authService.ask(AuthenticationActor.ValidateToken(token, traceId))(t)
          .mapTo[Try[ApiUser]]
          .andThen {
            case Success(res) ⇒
              log.tylog(Level.INFO, traceId, AuthenticateUser, Variation.Success, "validated token {}", token)
            case Failure(e) ⇒
              log.tylog(Level.INFO, traceId, AuthenticateUser, Variation.Failure(e), "token validation for token {} failed", token)
          }(mat.executionContext)
      }.flatMap {
        case Success(u) ⇒ provide(u)
        case Failure(e) ⇒ reject(AuthenticationFailed(e.getMessage))
      }
    }

}
