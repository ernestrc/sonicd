package build.unstable.sonicd.system.actor

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.http.scaladsl.model.DateTime
import akka.pattern._
import build.unstable.sonicd.api.auth.{ApiKey, ApiUser}
import build.unstable.sonicd.model.{Authenticate, Query}
import com.facebook.presto.jdbc.internal.guava.io.BaseEncoding

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.Try

class AuthenticationActor(apiKeys: List[ApiKey]) extends Actor with ActorLogging {

  import AuthenticationActor._
  import context.dispatcher

  val eventBus = context.system.eventStream

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    eventBus.subscribe(self, classOf[PropagateWithPeers])
  }

  val authenticationTokens = mutable.Map.empty[Token, ApiUser]

  def validateToken(token: Token): Try[ApiUser] = ???

  //authenticationTokens.get(token)

  def createToken(cmd: Authenticate): Try[Token] = ???

  /*{
    authenticationTokens.find(_._2 == user).map(_._1).map(Left.apply).getOrElse {
      apiKeys.find(_ == user.apiKey).map { key ⇒
        val t = encodeToken(user)
        authenticationTokens.update(t, user)
        eventBus.publish(PropagateWithPeers(t, user))
        Future.successful(t)
      }.getOrElse(Future.failed(new AuthenticationException(
        s"${user.apiKey} is not a valid api-key")))
    }
  }*/

  override def receive: Receive = {

    //authentication flow
    case PropagateWithPeers(t, user) ⇒ authenticationTokens.update(t, user)

    case ValidateTokenForQuery(token, query, handler) ⇒
      val u = validateToken(token)
      sender() ! SonicController.TokenValidationResult(u, query, handler)

    case ValidateToken(token) ⇒
      val u = validateToken(token)
      sender() ! u

    case cmd: Authenticate ⇒ sender() ! createToken(cmd)

  }
}

object AuthenticationActor {

  class AuthenticationException(msg: String) extends Exception

  val b64 = BaseEncoding.base64()

  type Token = String
  type ResourceId = String

  /*
  def encodeToken(user: ApiUser): Token = {
    b64.encode((user.user + "|" + user.apiKey + "|" + user.ip + "|" + new DateTime().toIsoDateTimeString()).getBytes)
  }

  def decodeToken(token: Token): ApiUser = {
    val decoded = new String(b64.decode(token), StandardCharsets.UTF_8)
    val user :: apikey :: ip :: _ = decoded.split("|").toList
    ApiUser(user, apikey, Some(ip))
  }*/

  case class ValidateTokenForQuery(token: Token, query: Query, handler: ActorRef)

  case class ValidateToken(token: Token)

  case class PropagateWithPeers(token: Token, user: ApiUser)

  case class AuthorizationConfirmed(id: String, user: ApiUser, until: DateTime)

  class InvalidApiKey extends Exception

}
