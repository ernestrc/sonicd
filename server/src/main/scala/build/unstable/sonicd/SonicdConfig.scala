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

  val JDBC_FETCHSIZE = Try(config.getInt("sonicd.jdbc.fetch-size")).getOrElse(1000)

  val AUTH_WORKERS: Int = config.getInt("sonicd.auth-workers")
  val AUTH_SECRET: String = config.getString("sonicd.auth-secret")
  val TOKEN_DURATION: FiniteDuration = FiniteDuration(config.getDuration("sonicd.token-duration").getSeconds,
    TimeUnit.SECONDS)

  assert(TOKEN_DURATION.isFinite() && TOKEN_DURATION > 1.minute,
    "token duration must be finite and greater than 1 minute")

  lazy val API_KEYS: List[ApiKey] =
    config.getList("sonicd.api-keys")
      .render(ConfigRenderOptions.concise())
      .parseJson match {
      case JsArray(v) ⇒ v.map(_.convertTo[ApiKey]).toList
      case _ ⇒ throw new Exception("'sonicd.api-keys' must be an array of JSON objects")
    }

  assert(API_KEYS.distinct.size == API_KEYS.size)

  lazy val SPARK_MASTER = Try(config.getString("sonicd.spark.master"))
  lazy val SPARK_HOME = Try(config.getString("sonicd.spark.home"))
  lazy val SPARK_DRIVER_CLASSPATH = Try(config.getString("sonicd.spark.driver.extraClassPath"))
  lazy val SPARK_DRIVER_LIBRARYPATH = Try(config.getString("sonicd.spark.driver.extraLibraryPath"))
  lazy val SPARK_EXECUTOR_CLASSPATH = Try(config.getString("sonicd.spark.executor.extraClassPath"))
  lazy val SPARK_EXECUTOR_LIBRARYPATH = Try(config.getString("sonicd.spark.executor.extraClassPath"))
  lazy val SPARK_JARS = Try(config.getStringList("sonicd.spark.jars").toSeq)

  lazy val ZUORA_MAX_NUMBER_RECORDS = Try(config.getInt("sonicd.zuora.query_limit")).getOrElse(2000)
  //https://knowledgecenter.zuora.com/DC_Developers/SOAP_API/E_SOAP_API_Calls/query_call
  assert(ZUORA_MAX_NUMBER_RECORDS <= 2000)

  lazy val ZUORA_QUERY_TIMEOUT = Duration(config.getDuration("sonicd.zuora.query-timeout").getSeconds, TimeUnit.SECONDS)
  lazy val ZUORA_ENDPOINT = config.getString("sonicd.zuora.endpoint")
  lazy val ZUORA_CONNECTION_POOL_SETTINGS = config.getConfig("sonicd.zuora")

  lazy val HADOOP_CONF = Try(config.getString("sonid.hadoop-config"))
  lazy val YARN_CONF = Try(config.getString("sonid.yarn-config"))

  implicit val ACTOR_TIMEOUT: Timeout = Timeout(config.getDuration("sonicd.actor-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)

  val ENDPOINT_TIMEOUT: Timeout = Timeout(config.getDuration("sonicd.endpoint-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)

  lazy val PRESTO_CONNECTION_POOL_SETTINGS = config.getConfig("sonicd.presto")
  lazy val PRESTO_RETRYIN: FiniteDuration = FiniteDuration(config.getDuration("sonicd.presto.retry-in").getSeconds, TimeUnit.SECONDS)
  lazy val PRESTO_MAX_RETRIES = config.getInt("sonicd.presto.max-retries")
  lazy val PRESTO_TIMEOUT = Duration(config.getDuration("sonicd.presto.timeout").getSeconds, TimeUnit.SECONDS)
  lazy val PRESTO_APIV = "v1"
}

