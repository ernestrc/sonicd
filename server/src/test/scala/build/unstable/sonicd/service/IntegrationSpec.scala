package build.unstable.sonicd.service

import java.net.InetSocketAddress
import java.sql.DriverManager

import akka.http.scaladsl.Http
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.io.Tcp
import akka.stream.scaladsl.Sink
import akka.testkit.TestKit
import build.unstable.sonicd.SonicdConfig
import build.unstable.sonicd.api.AkkaApi
import build.unstable.sonicd.model._
import build.unstable.sonicd.system.AkkaService
import com.typesafe.sslconfig.akka.AkkaSSLConfig
import com.typesafe.sslconfig.akka.util.AkkaLoggerFactory
import com.typesafe.sslconfig.ssl.{ConfigSSLContextBuilder, SSLConfigFactory}
import org.scalatest._
import spray.json.JsString

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/*
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

  //TODO
  "sonicd ws " should {
    "run a simple query using the ws api" in {
    }
  }

  "sonicd tcp " should {
    /*
      "run a simple query using the tcp api" in {

        val future: Future[Vector[SonicMessage]] = SonicdSource.run(tcpAddr, syntheticQuery)
        val stream: Future[DoneWithQueryExecution] =
          SonicdSource.stream(tcpAddr, syntheticQuery).to(Sink.ignore).run()

        val sDone = Await.result(stream, 20.seconds)
        val fDone = Await.result(future, 20.seconds)

        assert(sDone.success)
        fDone.length shouldBe 112 //1 metadata + 100 QueryProgress + 10 OutputChunk + 1 DoneWithQueryExecution
      }

      "run a query against a source that is configured server side" in {

        val syntheticQuery = new Query(None, "10", JsString("test_server_config"))
        val future: Future[Vector[SonicMessage]] = SonicdSource.run(tcpAddr, syntheticQuery)
        val stream: Future[DoneWithQueryExecution] =
          SonicdSource.stream(tcpAddr, syntheticQuery).to(Sink.ignore).run()

        val sDone = Await.result(stream, 20.seconds)
        val fDone = Await.result(future, 20.seconds)

        assert(sDone.success)
        fDone.length shouldBe 112 //1 metadata + 100 QueryProgress + 10 OutputChunk + 1 DoneWithQueryExecution
      }

      "should bubble exception thrown by source" in {
        val query = Query("select * from nonesense", H2Config) //table nonesense doesn't exist

        val future: Future[Vector[SonicMessage]] =
          SonicdSource.run(tcpAddr, query)
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
      }*/

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
}*/
