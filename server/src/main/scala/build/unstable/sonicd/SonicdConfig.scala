package build.unstable.sonicd

import java.util.concurrent.TimeUnit

import akka.util.Timeout
import build.unstable.sonicd.auth.ApiKey
import build.unstable.sonicd.model.SonicdLogging
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import spray.json._

import scala.collection.JavaConversions._
import scala.concurrent.duration.{Duration, FiniteDuration, _}
import scala.util.Try

object SonicdConfig extends FromResourcesConfig(ConfigFactory.load())

abstract class FromResourcesConfig(config: Config) extends SonicdLogging {

  val HTTP_PORT = config.getInt("sonicd.http-port")
  val TCP_PORT = config.getInt("sonicd.tcp-port")
  val INTERFACE = config.getString("sonicd.interface")

  val API_VERSION = "v1"

  val JDBC_FETCHSIZE = config.getInt("sonicd.jdbc.fetch-size")

  val CONTROLLERS: Int = config.getInt("sonicd.controllers")

  val AUTH_WORKERS: Int = config.getInt("sonicd.auth-workers")
  val AUTH_SECRET: String = config.getString("sonicd.auth-secret")
  val TOKEN_DURATION: FiniteDuration = FiniteDuration(config.getDuration("sonicd.token-duration").getSeconds,
    TimeUnit.SECONDS)

  assert(TOKEN_DURATION.isFinite() && TOKEN_DURATION > 1.minute,
    "token duration must be finite and greater than 1 minute")

  val API_KEYS: List[ApiKey] =
    config.getList("sonicd.api-keys")
      .render(ConfigRenderOptions.concise())
      .parseJson match {
      case JsArray(v) ⇒ v.map(_.convertTo[ApiKey]).toList
      case _ ⇒ throw new Exception("'sonicd.api-keys' must be an array of JSON objects")
    }

  assert(API_KEYS.distinct.size == API_KEYS.size)

  val ZUORA_MAX_FETCH_SIZE = Try(config.getInt("sonicd.zuora.query_limit")).getOrElse(2000)
  //https://knowledgecenter.zuora.com/DC_Developers/SOAP_API/E_SOAP_API_Calls/query_call
  assert(ZUORA_MAX_FETCH_SIZE <= 2000)

  val ZUORA_QUERY_TIMEOUT = Duration(config.getDuration("sonicd.zuora.query-timeout").getSeconds, TimeUnit.SECONDS)
  val ZUORA_HTTP_ENTITY_TIMEOUT = Duration(config.getDuration("sonicd.zuora.http-entity-timeout").getSeconds, TimeUnit.SECONDS)
  val ZUORA_ENDPOINT = config.getString("sonicd.zuora.endpoint")
  val ZUORA_CONNECTION_POOL_SETTINGS = config.getConfig("sonicd.zuora")

  implicit val ACTOR_TIMEOUT: Timeout = Timeout(config.getDuration("sonicd.actor-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)

  val ENDPOINT_TIMEOUT: Timeout = Timeout(config.getDuration("sonicd.endpoint-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)

  val PRESTO_CONNECTION_POOL_SETTINGS = config.getConfig("sonicd.presto")
  val PRESTO_RETRYIN: FiniteDuration = FiniteDuration(config.getDuration("sonicd.presto.retry-in").getSeconds, TimeUnit.SECONDS)
  val PRESTO_RETRY_MULTIPLIER: Int = config.getInt("sonicd.presto.retry-multiplier")
  val PRESTO_HTTP_ENTITY_TIMEOUT = Duration(config.getDuration("sonicd.presto.http-entity-timeout").getSeconds, TimeUnit.SECONDS)
  val PRESTO_MAX_RETRIES = config.getInt("sonicd.presto.max-retries")
  val PRESTO_TIMEOUT = Duration(config.getDuration("sonicd.presto.timeout").getSeconds, TimeUnit.SECONDS)
  val PRESTO_APIV = "v1"
  val PRESTO_WATERMARK = config.getInt("sonicd.presto.watermark")

  val ES_CONNECTION_POOL_SETTINGS = config.getConfig("sonicd.es")
  val ES_HTTP_ENTITY_TIMEOUT = Duration(config.getDuration("sonicd.es.http-entity-timeout").getSeconds, TimeUnit.SECONDS)
  val ES_WATERMARK = config.getLong("sonicd.es.watermark")
  val ES_QUERY_SIZE = config.getLong("sonicd.es.query-size")

  assert(ES_WATERMARK < ES_QUERY_SIZE, "ES watermark must be smaller than query fetch size")
}

