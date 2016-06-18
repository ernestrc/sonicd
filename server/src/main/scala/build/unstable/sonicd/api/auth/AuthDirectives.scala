package build.unstable.sonicd.api.auth

import akka.actor.ActorRef
import akka.http.scaladsl.server.Directive.SingleValueModifiers
import akka.http.scaladsl.server.{Directive1, Directives, Rejection}
import akka.pattern.ask
import akka.util.Timeout
import build.unstable.sonicd.model.{JsonProtocol, Authenticate}
import build.unstable.sonicd.system.actor.AuthenticationActor
import spray.json.RootJsonFormat
import Directives._
import spray.json._
import JsonProtocol._

import scala.util.{Success, Failure, Try}

object AuthDirectives {

  case class AuthenticationFailed(msg: String) extends Rejection

  case class LoginFailed(e: Throwable) extends Rejection

  implicit val authJsonFormatter: RootJsonFormat[Authenticate] = jsonFormat2(Authenticate.apply)

  def createAuthToken(authService: ActorRef, t: Timeout): Directive1[AuthenticationActor.Token] =
    entity(as[Authenticate]).flatMap { authCmd =>
      onSuccess(authService.ask(authCmd)(t).mapTo[Try[AuthenticationActor.Token]]).flatMap {
        case Success(token) ⇒ provide(token)
        case Failure(e) ⇒ reject(LoginFailed(e))
      }
    }

  def tokenFromHeaderAuthentication(authService: ActorRef, t: Timeout): Directive1[ApiUser] =
    headerValueByName("SONICD-AUTH").flatMap { token ⇒
      onSuccess(authService.ask(AuthenticationActor.ValidateToken(token))(t).mapTo[Option[ApiUser]]).flatMap {
        case Some(u) ⇒ provide(u)
        case None ⇒ reject(AuthenticationFailed("unauthorized token"))
      }
    }

}
