package build.unstable.sonicd.service

import java.net.InetAddress

import akka.actor.{Terminated, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import akka.util.Timeout
import build.unstable.sonicd.api.auth.{ApiUser, Mode, ApiKey}
import build.unstable.sonicd.model.{DoneWithQueryExecution, Query}
import build.unstable.sonicd.system.actor.AuthenticationActor.ValidateToken
import build.unstable.sonicd.system.actor.{AuthenticationActor, SonicController}
import build.unstable.sonicd.system.actor.SonicController.NewQuery
import com.auth0.jwt.JWTSigner
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import spray.json._

import scala.concurrent.duration._

class SonicControllerSpec(_system: ActorSystem) extends TestKit(_system)
with WordSpecLike with Matchers with BeforeAndAfterAll with ImplicitSender {

  def this() = this(ActorSystem("TcpHandlerSpec"))

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  def newActor: TestActorRef[SonicController] =
    TestActorRef[SonicController](Props(classOf[SonicController], self, 1.seconds:Timeout))

  val signer = new JWTSigner("secret")

  "SonicController" should {
    "handle new queries and monitor handler" in {
      val c = newActor

      c ! NewQuery(build.unstable.sonicd.model.Fixture.syntheticQuery, None)
      expectMsgType[Props]

      c.underlyingActor.handled shouldBe 1
      c.underlyingActor.handlers(1) shouldBe self.path

      c ! Terminated(self)(false, true)
      assert(c.underlyingActor.handlers.isEmpty)
    }

    "authorize queries on sources with security" in {
      val c = newActor
      val config = """{"class" : "SyntheticSource", "security" : 1}""".parseJson.asJsObject
      val claims = ApiKey("1", Mode.Read, 1, None, None).toClaims("bandit")
      val user = ApiUser.fromClaims(claims)
      val auth = signer.sign(claims)
      val syntheticQuery = Query("10", config, Some(auth)).copy(trace_id = Some("1234"))

      c ! NewQuery(syntheticQuery, None)
      val cmd = expectMsgType[ValidateToken]
      assert(cmd.token == auth)

      lastSender ! user
      expectMsgType[Props]

      c.underlyingActor.handled shouldBe 1

    }

    "reject queries on sources with security that are either unauthenticated or don't have enought authorization" in {
      {
        val c = newActor
        val config = """{"class" : "SyntheticSource", "security" : 2}""".parseJson.asJsObject
        val claims = ApiKey("1", Mode.Read, 1, None, None).toClaims("bandit")
        val user = ApiUser.fromClaims(claims)
        val auth = signer.sign(claims)
        val syntheticQuery = Query("10", config, Some(auth)).copy(trace_id = Some("1234"))

        c ! NewQuery(syntheticQuery, None)
        val cmd = expectMsgType[ValidateToken]
        assert(cmd.token == auth)

        lastSender ! user
        val done = expectMsgType[DoneWithQueryExecution]

        done.errors.head.isInstanceOf[AuthenticationActor.AuthenticationException]

        c.underlyingActor.handled shouldBe 1
        assert(c.underlyingActor.handlers.isEmpty)
      }

      {
        val c = newActor
        val config = """{"class" : "SyntheticSource", "security" : 2}""".parseJson.asJsObject
        val syntheticQuery = Query("10", config, None).copy(trace_id = Some("1234"))

        c ! NewQuery(syntheticQuery, None)

        val done = expectMsgType[DoneWithQueryExecution]

        done.errors.head.isInstanceOf[AuthenticationActor.AuthenticationException]

        c.underlyingActor.handled shouldBe 1
        assert(c.underlyingActor.handlers.isEmpty)
      }
    }

    "reject queries on sources with ip-blocking enabled from clients that are not in whitelist" in {
      val c = newActor
      val config = """{"class" : "SyntheticSource", "security" : 1}""".parseJson.asJsObject
      val allowedIps = InetAddress.getByName("10.0.0.1") :: Nil
      val claims = ApiKey("1", Mode.Read, 1, Some(allowedIps), None).toClaims("bandit")
      val user = ApiUser.fromClaims(claims)
      val auth = signer.sign(claims)
      val syntheticQuery = Query("10", config, Some(auth)).copy(trace_id = Some("1234"))

      c ! NewQuery(syntheticQuery, Some(InetAddress.getByName("localhost")))
      val cmd = expectMsgType[ValidateToken]
      assert(cmd.token == auth)

      lastSender ! user
      val done = expectMsgType[DoneWithQueryExecution]

      done.errors.head.isInstanceOf[AuthenticationActor.AuthenticationException]

      c.underlyingActor.handled shouldBe 1
      assert(c.underlyingActor.handlers.isEmpty)
    }

    "accept queries on sources with ip-blocking enabled from clients that are whitelisted" in {
      val c = newActor
      val config = """{"class" : "SyntheticSource", "security" : 1}""".parseJson.asJsObject
      val allowedIps = InetAddress.getByName("localhost") :: Nil
      val claims = ApiKey("1", Mode.Read, 1, Some(allowedIps), None).toClaims("bandit")
      val user = ApiUser.fromClaims(claims)
      val auth = signer.sign(claims)
      val syntheticQuery = Query("10", config, Some(auth)).copy(trace_id = Some("1234"))

      c ! NewQuery(syntheticQuery, Some(allowedIps.head))
      val cmd = expectMsgType[ValidateToken]
      assert(cmd.token == auth)

      lastSender ! user
      expectMsgType[Props]

      c.underlyingActor.handled shouldBe 1
    }
  }
}