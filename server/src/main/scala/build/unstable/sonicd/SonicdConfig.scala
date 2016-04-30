package build.unstable.sonicd

import java.util.concurrent.TimeUnit

import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import scala.collection.JavaConversions._
import scala.concurrent.duration.Duration

import scala.util.Try

object SonicdConfig extends FromResourcesConfig(ConfigFactory.load())

abstract class FromResourcesConfig(config: Config) {

  val DEV: Boolean = config.getBoolean("sonic.dev")

  val HTTP_PORT = config.getInt("sonic.http-port")
  val TCP_PORT = config.getInt("sonic.tcp-port")
  val INTERFACE = config.getString("sonic.interface")

  val API_VERSION = "v1"

  val JDBC_FETCHSIZE = Try(config.getInt("jdbc.fetch-size")).getOrElse(1000)

  lazy val SPARK_MASTER = Try(config.getString("spark.master"))
  lazy val SPARK_HOME = Try(config.getString("spark.home"))
  lazy val SPARK_DRIVER_CLASSPATH = Try(config.getString("spark.driver.extraClassPath"))
  lazy val SPARK_DRIVER_LIBRARYPATH = Try(config.getString("spark.driver.extraLibraryPath"))
  lazy val SPARK_EXECUTOR_CLASSPATH = Try(config.getString("spark.executor.extraClassPath"))
  lazy val SPARK_EXECUTOR_LIBRARYPATH = Try(config.getString("spark.executor.extraClassPath"))
  lazy val SPARK_JARS = Try(config.getStringList("spark.jars").toSeq)

  lazy val ZUORA_MAX_NUMBER_RECORDS = Try(config.getInt("zuora.query_limit")).getOrElse(2000)
  //https://knowledgecenter.zuora.com/DC_Developers/SOAP_API/E_SOAP_API_Calls/query_call
  assert(ZUORA_MAX_NUMBER_RECORDS <= 2000)

  lazy val ZUORA_QUERY_TIMEOUT = Duration(config.getDuration("zuora.query-timeout").getSeconds, TimeUnit.SECONDS)
  lazy val ZUORA_ENDPOINT = config.getString("zuora.endpoint")
  lazy val ZUORA_CONNECTION_POOL_SETTINGS = config.getConfig("zuora")

  lazy val HADOOP_CONF = Try(config.getString("sonic.hadoop-config"))
  lazy val YARN_CONF = Try(config.getString("sonic.yarn-config"))

  implicit val ACTOR_TIMEOUT: Timeout = Timeout(config.getDuration("sonic.actor-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)

  val ENDPOINT_TIMEOUT: Timeout = Timeout(config.getDuration("sonic.endpoint-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)

  lazy val PRESTO_CONNECTION_POOL_SETTINGS = config.getConfig("presto")
  lazy val PRESTO_RETRYIN = config.getInt("presto.retry-in")
  lazy val PRESTO_RETRY_MULTIPLIER = config.getInt("presto.retry-multiplier")
  lazy val PRESTO_TIMEOUT = Duration(config.getDuration("presto.timeout").getSeconds, TimeUnit.SECONDS)
  lazy val PRESTO_APIV = "v1"
}

