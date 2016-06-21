package build.unstable.sonicd.system.actor

import akka.actor.{Actor, ActorLogging}
import akka.http.scaladsl.model.DateTime
import build.unstable.sonicd.api.auth.{ApiKey, ApiUser}
import build.unstable.sonicd.model.Authenticate
import com.auth0.jwt.{JWTSigner, JWTVerifier}

import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal
import scala.util.{Failure, Try}

class AuthenticationActor(apiKeys: List[ApiKey], secret: String,
                          globalTokenDuration: FiniteDuration) extends Actor with ActorLogging {

  import AuthenticationActor._

  val signer = new JWTSigner(secret)
  val verifier = new JWTVerifier(secret)

  def validateToken(token: Token): Try[ApiUser] = {
    Try(verifier.verify(token)).recover {
      case NonFatal(e) ⇒ throw new TokenVerificationFailed(e)
    }.flatMap { verifiedClaims ⇒
      val apiKey = verifiedClaims.get("key").asInstanceOf[String]
      apiKeys.find(_.key == apiKey)
        .map(_ ⇒ ApiUser.fromClaims(verifiedClaims))
        .getOrElse(Failure(new AuthenticationException(s"invalid token: unknown api-key")))
    }
  }

  def createToken(key: String, user: String): Try[Token] = {
    apiKeys.find(_.key == key).map { apiKey ⇒
      Try {
        val signOpts = new JWTSigner.Options()
        //expiry
        val seconds = apiKey.tokenExpires.getOrElse(globalTokenDuration)

        signOpts.setExpirySeconds(seconds.toSeconds.toInt)
        val claims = apiKey.toClaims(user)
        signer.sign(claims, signOpts)
      }
    }.getOrElse(Failure(new AuthenticationException(s"invalid api-key: $key")))
  }

  override def receive: Receive = {

    case ValidateToken(token) ⇒ sender() ! validateToken(token)

    case Authenticate(user, key) ⇒ sender() ! createToken(key, user)

  }
}

object AuthenticationActor {

  type Token = String

  case class ValidateToken(token: Token)

  case class AuthorizationConfirmed(id: String, user: ApiUser, until: DateTime)

  class AuthenticationException(msg: String) extends Exception(msg)

  class TokenVerificationFailed(inner: Throwable) extends Exception("token verification failed", inner)

}
