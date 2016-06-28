package build.unstable.sonicd.service

import java.net.InetAddress
import java.util.concurrent.TimeUnit

import akka.actor.{ActorSystem, Props}
import akka.testkit.{CallingThreadDispatcher, ImplicitSender, TestActorRef, TestKit}
import build.unstable.sonicd.auth.{ApiKey, ApiUser}
import build.unstable.sonicd.model.Authenticate
import build.unstable.sonicd.system.actor.AuthenticationActor
import com.auth0.jwt.JWTSigner
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.Try

class AuthenticationActorSpec(_system: ActorSystem) extends TestKit(_system)
with WordSpecLike with Matchers with BeforeAndAfterAll with ImplicitSender {

  def this() = this(ActorSystem("AuthenticationActorSpec"))

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  val apiKeys = ApiKey("1", ApiKey.Mode.Read, 1, Some(List(InetAddress.getByName("localhost"))), None) ::
    ApiKey("2", ApiKey.Mode.ReadWrite, 3, None, None) :: Nil
  val tokenExpiration: FiniteDuration = 10.seconds
  val secret = "super-secret"

  def newActor(keys: List[ApiKey] = apiKeys,
               secret: String = secret,
               tokenExpiration: FiniteDuration = tokenExpiration): TestActorRef[AuthenticationActor] = {
    TestActorRef[AuthenticationActor](Props(classOf[AuthenticationActor], apiKeys, secret, tokenExpiration)
    .withDispatcher(CallingThreadDispatcher.Id))
  }

  "Authentication actor" should {
    "create tokens from authenticate commands when api key is valid" in {
      val actor = newActor()

      actor ! Authenticate("pepito", "1", Some("1"))
      val tokenMaybe = expectMsgType[Try[String]]

      val token = tokenMaybe.get

      val verified = actor.underlyingActor.verifier.verify(token)
      val user = ApiUser.fromJWTClaims(verified)

      assert(user.get.authorization == 1)
      assert(user.get.mode == ApiKey.Mode.Read)
      assert(user.get.user == "pepito")
      assert(user.get.allowedIps.get == List(InetAddress.getByName("localhost")))
    }

    "reject invalid api keys" in {
      val actor = newActor()
      actor ! Authenticate("grillo", "invalidKey", Some("1"))
      val tokenMaybe = expectMsgType[Try[String]]
      assert(tokenMaybe.isFailure)

      assert(tokenMaybe.failed.get.isInstanceOf[AuthenticationActor.AuthenticationException])
    }

    "verify token and return logged user if verification was successful" in {
      val actor = newActor()
      val token = actor.underlyingActor.signer.sign(apiKeys.head.toJWTClaims("serrallonga"))

      actor ! AuthenticationActor.ValidateToken(token, "1")
      val res = expectMsgType[Try[AuthenticationActor.Token]]
      res.get
    }

    "verify token and return failure if token is not JWT" in {
      val actor = newActor()

      actor ! AuthenticationActor.ValidateToken("invalidToken", "2")
      val res = expectMsgType[Try[AuthenticationActor.Token]]
      assert(res.isFailure)

      assert(res.failed.get.isInstanceOf[AuthenticationActor.TokenVerificationFailed])

    }

    "verify token and return failure if token expired" in {
      {
        val tokenDuration = FiniteDuration.apply(1, TimeUnit.SECONDS)
        val actor = newActor(tokenExpiration = tokenDuration)
        val opts = new JWTSigner.Options()
        opts.setExpirySeconds(tokenDuration.toSeconds.toInt)
        val apiKey = apiKeys.head
        val token = actor.underlyingActor.signer.sign(apiKey.toJWTClaims("serrallonga"), opts)

        //expire token
        Thread.sleep(1000)

        actor ! AuthenticationActor.ValidateToken(token, "2")
        val res = expectMsgType[Try[AuthenticationActor.Token]]
        assert(res.isFailure)

        assert(res.failed.get.isInstanceOf[AuthenticationActor.TokenExpired])
      }

      {
        val tokenDuration = FiniteDuration.apply(1, TimeUnit.SECONDS)
        val actor = newActor()
        val opts = new JWTSigner.Options()
        opts.setExpirySeconds(tokenDuration.toSeconds.toInt)
        val apiKey = apiKeys.head.copy(tokenExpires = Some(tokenDuration))
        val token = actor.underlyingActor.signer.sign(apiKey.toJWTClaims("serrallonga"), opts)

        //expire token
        Thread.sleep(1000)

        actor ! AuthenticationActor.ValidateToken(token, "2")
        val res = expectMsgType[Try[AuthenticationActor.Token]]
        assert(res.isFailure)

        assert(res.failed.get.isInstanceOf[AuthenticationActor.TokenExpired])
      }

    }
  }
}
