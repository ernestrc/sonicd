package build.unstable.sonicd

import java.net.InetSocketAddress

import akka.http.scaladsl.Http
import akka.io.Tcp
import build.unstable.sonicd.api.AkkaApi
import build.unstable.sonicd.system.{AkkaService, AkkaSystem}
import com.typesafe.sslconfig.akka.AkkaSSLConfig
import com.typesafe.sslconfig.akka.util.AkkaLoggerFactory
import com.typesafe.sslconfig.ssl.{ConfigSSLContextBuilder, SSLConfigFactory}
import org.slf4j.LoggerFactory

object Sonic extends App with AkkaSystem with AkkaService with AkkaApi {

  val log = LoggerFactory.getLogger(this.getClass)

  val http = Http()
  val sslConfigFactory = AkkaSSLConfig()

  val sonicOverrides = system.settings.config.getConfig("sonic.ssl-config")
  val defaults = system.settings.config.getConfig("ssl-config")
  val config = SSLConfigFactory.parse(sonicOverrides withFallback defaults)

  val keyManagerFactory = sslConfigFactory.buildKeyManagerFactory(config)
  val trustManagerFactory = sslConfigFactory.buildTrustManagerFactory(config)
  val sslContext = new ConfigSSLContextBuilder(new AkkaLoggerFactory(system), config, keyManagerFactory, trustManagerFactory).build()

  http.bindAndHandle(handler = httpHandler, interface = SonicConfig.INTERFACE, port = SonicConfig.HTTP_PORT)

  tcpIoService.tell(Tcp.Bind(tcpService,
    new InetSocketAddress(SonicConfig.INTERFACE, SonicConfig.TCP_PORT), options = Nil, pullMode = true), tcpService)

  log.info(s"STARTING SONIC SERVICE V.${BuildInfo.version} (${BuildInfo.commit} ${BuildInfo.builtAt}) " +
    s"on interface ${SonicConfig.INTERFACE}; http port: ${SonicConfig.HTTP_PORT}; tcp port: ${SonicConfig.TCP_PORT}")

  log.info(s"ssl config: $config with default protocol: ${config.protocol}")

}
