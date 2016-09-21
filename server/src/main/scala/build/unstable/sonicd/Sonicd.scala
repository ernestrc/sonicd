package build.unstable.sonicd

import java.net.InetSocketAddress

import akka.http.scaladsl.Http
import akka.io.Tcp
import akka.stream.scaladsl.{Tcp ⇒ StreamTcp}
import build.unstable.sonicd.api.AkkaApi
import build.unstable.sonicd.system.{AkkaService, AkkaSystem}
import com.typesafe.sslconfig.akka.AkkaSSLConfig
import com.typesafe.sslconfig.akka.util.AkkaLoggerFactory
import com.typesafe.sslconfig.ssl.{ConfigSSLContextBuilder, SSLConfigFactory}

object Sonicd extends App with AkkaSystem with AkkaService with AkkaApi with SonicdLogging {

  val http = Http()
  val sslConfigFactory = AkkaSSLConfig()

  val sonicOverrides = system.settings.config.getConfig("sonicd.ssl-config")
  val defaults = system.settings.config.getConfig("ssl-config")
  val config = SSLConfigFactory.parse(sonicOverrides withFallback defaults)

  val keyManagerFactory = sslConfigFactory.buildKeyManagerFactory(config)
  val trustManagerFactory = sslConfigFactory.buildTrustManagerFactory(config)
  val sslContext = new ConfigSSLContextBuilder(new AkkaLoggerFactory(system), config, keyManagerFactory, trustManagerFactory).build()

  http.bindAndHandle(handler = httpHandler, interface = SonicdConfig.INTERFACE, port = SonicdConfig.HTTP_PORT)

  tcpIoService.tell(Tcp.Bind(tcpService,
    new InetSocketAddress(SonicdConfig.INTERFACE, SonicdConfig.TCP_PORT), options = Nil, pullMode = true), tcpService)

  log.info( "STARTING SONIC SERVICE V.{} ({} {}) on interface {}; http port: {}; tcp port: {}",
    BuildInfo.version, BuildInfo.commit, BuildInfo.builtAt, SonicdConfig.INTERFACE,
    SonicdConfig.HTTP_PORT, SonicdConfig.TCP_PORT)

  log.info( "ssl config: {} with default protocol: {}", config, config.protocol)

}
