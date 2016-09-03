package build.unstable.sonicd.system.actor

import akka.actor.Actor
import akka.http.scaladsl.model.DateTime
import build.unstable.sonic.Authenticate
import build.unstable.sonicd.auth.{ApiKey, ApiUser}
import build.unstable.sonicd.model.SonicdLogging
import build.unstable.tylog.Variation
import com.auth0.jwt.{JWTSigner, JWTVerifier}

import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal
import scala.util.{Failure, Try}

class AuthenticationActor(apiKeys: List[ApiKey], secret: String,
                          globalTokenDuration: FiniteDuration)
  extends Actor with SonicdLogging {

  import AuthenticationActor._

  val signer = new JWTSigner(secret)
  val verifier = new JWTVerifier(secret)

  def validateToken(token: Token, traceId: String): Try[ApiUser] = {
    Try {
      try {
        trace(log, traceId, JWTVerifyToken, Variation.Attempt, "verifying token {}", token)
        val verifiedClaims = verifier.verify(token)
        trace(log, traceId, JWTVerifyToken, Variation.Success, "token is valid {}", token)
        verifiedClaims
      } catch {
        case e: com.auth0.jwt.JWTExpiredException ⇒
          trace(log, traceId, JWTVerifyToken, Variation.Failure(e), "token {} expired", token)
          throw new TokenExpired(e)
        case NonFatal(e) ⇒
          trace(log, traceId, JWTVerifyToken, Variation.Failure(e), "token is not valid {}", token)
          throw new TokenVerificationFailed(e)
      }
    }.flatMap(ApiUser.fromJWTClaims)
  }

  def createToken(key: String, user: String, traceId: String): Try[Token] = {
    apiKeys.find(_.key == key).map { apiKey ⇒
      Try {
        val signOpts = new JWTSigner.Options()
        //expiry
        val seconds = apiKey.tokenExpires.getOrElse(globalTokenDuration)

        signOpts.setExpirySeconds(seconds.toSeconds.toInt)
        val claims = apiKey.toJWTClaims(user)
        trace(log, traceId, JWTSignToken, Variation.Attempt,
          "signing token for {} with api key {} with expiration {}(s)", user, key, seconds)
        try {
          val token = signer.sign(claims, signOpts)
          trace(log, traceId, JWTSignToken, Variation.Success, "signed new token {}", token)
          token
        } catch {
          case NonFatal(e) ⇒
            trace(log, traceId, JWTSignToken, Variation.Failure(e), "failed to sign token for key {} and user {}", key, user)
            throw e
        }
      }
    }.getOrElse(Failure(new AuthenticationException(s"invalid api-key: $key")))
  }

  override def receive: Receive = {

    case v: ValidateToken ⇒ sender() ! validateToken(v.token, v.traceId)

    case cmd@Authenticate(user, key, traceId) ⇒
      sender() ! createToken(key, user, traceId.get)

  }
}

object AuthenticationActor {

  type Token = String

  case class ValidateToken(token: Token, traceId: String)

  case class AuthorizationConfirmed(id: String, user: ApiUser, until: DateTime)

  class AuthenticationException(msg: String) extends Exception(msg)

  class TokenVerificationFailed(inner: Throwable) extends Exception("token verification failed", inner)
  
  class TokenExpired(cause: com.auth0.jwt.JWTExpiredException)
    extends Exception("token expired. generate a new one and try again", cause)

}
