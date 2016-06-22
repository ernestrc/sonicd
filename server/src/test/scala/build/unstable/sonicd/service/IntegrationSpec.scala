package build.unstable.sonicd.service

import java.net.{InetAddress, InetSocketAddress}
import java.sql.DriverManager

import akka.http.scaladsl.Http
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.io.Tcp
import akka.stream.scaladsl.Sink
import akka.testkit.TestKit
import build.unstable.sonicd.SonicdConfig
import build.unstable.sonicd.api.AkkaApi
import build.unstable.sonicd.auth.ApiKey
import build.unstable.sonicd.model._
import build.unstable.sonicd.system.AkkaService
import com.auth0.jwt.{JWTSigner, JWTVerifier}
import com.typesafe.sslconfig.akka.AkkaSSLConfig
import com.typesafe.sslconfig.akka.util.AkkaLoggerFactory
import com.typesafe.sslconfig.ssl.{ConfigSSLContextBuilder, SSLConfigFactory}
import org.scalatest._
import spray.json.JsString

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class IntegrationSpec extends WordSpec with Matchers with ScalatestRouteTest
with BeforeAndAfterAll with TestSystem with AkkaService with AkkaApi
with JsonProtocol with SonicdLogging {

  import Fixture._
  import build.unstable.sonicd.model.Fixture._

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    testConnection.close()
  }

  val http = Http()
  val sslConfigFactory = AkkaSSLConfig()

  val sonicOverrides = system.settings.config.getConfig("sonicd.ssl-config")
  val defaults = system.settings.config.getConfig("ssl-config")
  val config = SSLConfigFactory.parse(sonicOverrides withFallback defaults)

  val keyManagerFactory = sslConfigFactory.buildKeyManagerFactory(config)
  val trustManagerFactory = sslConfigFactory.buildTrustManagerFactory(config)
  val sslContext = new ConfigSSLContextBuilder(new AkkaLoggerFactory(system), config, keyManagerFactory, trustManagerFactory).build()

  http.bindAndHandle(handler = httpHandler, interface = SonicdConfig.INTERFACE, port = SonicdConfig.HTTP_PORT)

  val tcpAddr = new InetSocketAddress(SonicdConfig.INTERFACE, SonicdConfig.TCP_PORT)

  tcpIoService.tell(Tcp.Bind(tcpService, tcpAddr, options = Nil, pullMode = true), tcpService)

  Class.forName(H2Driver)
  val testConnection = DriverManager.getConnection(H2Url, "SONICD", "")

  /* is tested with the node bindings
  "sonicd ws api" should {
  }*/

  "sonicd tcp api" should {
    "run a simple query using the tcp api" in {

      val future: Future[Vector[SonicMessage]] = SonicdSource.run(syntheticQuery, tcpAddr)
      val stream: Future[DoneWithQueryExecution] =
        SonicdSource.stream(tcpAddr, syntheticQuery).to(Sink.ignore).run()

      val sDone = Await.result(stream, 20.seconds)
      val fDone = Await.result(future, 20.seconds)

      assert(sDone.success)
      fDone.length shouldBe 112 //1 metadata + 100 QueryProgress + 10 OutputChunk + 1 DoneWithQueryExecution
    }

    "run a query against a source that is configured server side" in {

      val syntheticQuery = new Query(None, None, None, "10", JsString("test_server_config"))
      val future: Future[Vector[SonicMessage]] = SonicdSource.run(syntheticQuery, tcpAddr)
      val stream: Future[DoneWithQueryExecution] =
        SonicdSource.stream(tcpAddr, syntheticQuery).to(Sink.ignore).run()

      val sDone = Await.result(stream, 20.seconds)
      assert(sDone.success)

      val fDone = Await.result(future, 20.seconds)
      fDone.length shouldBe 112 //1 metadata + 100 QueryProgress + 10 OutputChunk + 1 DoneWithQueryExecution
    }

    val verifier = new JWTVerifier(SonicdConfig.AUTH_SECRET)
    val signer = new JWTSigner(SonicdConfig.AUTH_SECRET)

    "authenticate a user" in {

      val future: Future[String] =
        SonicdSource.authenticate("serrallonga", SonicdConfig.API_KEYS.head.key, tcpAddr)

      val token = Await.result(future, 20.seconds)
      assert(!verifier.verify(token).isEmpty)
    }

    "reject a user authentication attempt if apiKey is invalid" in {

      val future: Future[String] = SonicdSource.authenticate("serrallonga", "INVALID", tcpAddr)

      val e = intercept[Exception]{
        Await.result(future, 20.seconds)
      }
      assert(e.getMessage.contains("INVALID"))
    }

    "reject a query of a source that requires authentication if user is unauthenticated" in {
      val syntheticQuery = Query("10", JsString("secure_server_config"), None)
      val future: Future[Vector[SonicMessage]] = SonicdSource.run(syntheticQuery, tcpAddr)

      val e = intercept[Exception]{
        Await.result(future, 20.seconds)
      }
      assert(e.getMessage.contains("unauthenticated"))
    }

    "accept a query of a source that requires authentication if user is authenticated with at least the sources security level" in {
      val token = signer.sign(ApiKey("1234", ApiKey.Mode.Read, 5, None, None).toJWTClaims("bandit"))
      val syntheticQuery = Query("10", JsString("secure_server_config"), Some(token))

      val future: Future[Vector[SonicMessage]] = SonicdSource.run(syntheticQuery, tcpAddr)

      val fDone = Await.result(future, 20.seconds)
      fDone.length shouldBe 112
    }

    "reject a query of a source that requires authentication if user is authenticated with a lower authorization level" in {
      val token = signer.sign(ApiKey("1234", ApiKey.Mode.Read, 4, None, None).toJWTClaims("bandit"))
      val syntheticQuery = Query("10", JsString("secure_server_config"), Some(token))
      val future: Future[Vector[SonicMessage]] = SonicdSource.run(syntheticQuery, tcpAddr)

      val e = intercept[Exception]{
        Await.result(future, 20.seconds)
      }

      assert(e.getMessage.contains("unauthorized"))
    }

    "accept a query of a source that requires authentication and has a whitelist of ips and user ip is in the list" in {
      val token = signer.sign(ApiKey("1234", ApiKey.Mode.Read, 6,
        Some(InetAddress.getByName("127.0.0.1") :: Nil), None).toJWTClaims("bandit"))
      val syntheticQuery = Query("10", JsString("secure_server_config"), Some(token))
      val future: Future[Vector[SonicMessage]] = SonicdSource.run(syntheticQuery, tcpAddr)

      val fDone = Await.result(future, 20.seconds)
      fDone.length shouldBe 112
    }

    "reject a query of a source that requires authentication and has a whitelist of ips but user ip is not in the list" in {
      val token = signer.sign(ApiKey("1234", ApiKey.Mode.Read, 6,
        Some(InetAddress.getByName("192.168.1.17") :: Nil), None).toJWTClaims("bandit"))
      val syntheticQuery = Query("10", JsString("secure_server_config"), Some(token))
      val future: Future[Vector[SonicMessage]] = SonicdSource.run(syntheticQuery, tcpAddr)

      intercept[Exception]{
        Await.result(future, 20.seconds)
      }
    }

    "should bubble exception thrown by source" in {
      val query = Query("select * from nonesense", H2Config, None) //table nonesense doesn't exist

      val future: Future[Vector[SonicMessage]] =
        SonicdSource.run(query, tcpAddr)
      val stream: Future[DoneWithQueryExecution] =
        SonicdSource.stream(tcpAddr, query).to(Sink.ignore).run()

      val sThrown = intercept[Exception] {
          Await.result(stream, 20.seconds)
        }
      assert(sThrown.getMessage.contains("not found"))

      val thrown = intercept[Exception] {
          Await.result(future, 20.seconds)
        }
      assert(thrown.getMessage.contains("not found"))
    }

    /*
    "should bubble exception thrown by the tcp stage" in {

      val future: Future[Vector[SonicMessage]] =
        SonicdSource.run(new InetSocketAddress(SonicdConfig.INTERFACE, SonicdConfig.TCP_PORT + 1), syntheticQuery)

      val stream: Future[DoneWithQueryExecution] =
        SonicdSource.stream(new InetSocketAddress(SonicdConfig.INTERFACE, SonicdConfig.TCP_PORT + 1), syntheticQuery)
          .to(Sink.ignore).run()

      val thrown = intercept[java.net.ConnectException] {
        Await.result(future, 20.seconds)
      }
      assert(thrown.getMessage.contains("Could not establish connection to"))

      val sThrown = intercept[java.net.ConnectException] {
        Await.result(stream, 20.seconds)
      }
      assert(sThrown.getMessage.contains("Could not establish connection to"))

    }*/
  }
}
