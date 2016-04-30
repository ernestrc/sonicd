package build.unstable.sonicd.source

import java.net.URI

import akka.actor._
import akka.stream.ActorMaterializer
import akka.stream.actor.ActorPublisher
import akka.stream.scaladsl.StreamConverters.fromInputStream
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.ByteString
import build.unstable.sonicd.SonicdConfig
import build.unstable.sonicd.model._
import org.apache.commons.io.IOUtils
import org.apache.spark.launcher.SparkLauncher
import spray.json.{JsBoolean, JsObject, JsString}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.reflect.io.File
import scala.util.{Failure, Success, Try}

class SparkSQLSource(config: JsObject, queryId: String, query: String, actorContext: ActorContext)
  extends DataSource(config, queryId, query, actorContext) {

  import build.unstable.sonicd.model.JsonProtocol._

  val handlerProps: Props = {
    val cfg = config.fields
    val jarLocation = cfg.getOrElse("jar", JsString("spark_2.11-assembly.jar")).convertTo[String]
    val mainClass = cfg.getOrElse("mainClass", JsString("build.unstable.sonic.spark.jobs.SparkSQL")).convertTo[String]
    val printColNames: Boolean = cfg.getOrElse("column-names", JsBoolean(false)).convertTo[Boolean]

    Props(classOf[SparkPublisher], queryId, query, printColNames, jarLocation, mainClass)
  }
}

class SparkPublisher(queryId: String, query: String, printColNames: Boolean, jar: String, mainClass: String)
  extends ActorPublisher[SonicMessage] with ActorLogging {

  import akka.stream.actor.ActorPublisherMessage._
  import context.dispatcher

  override def postStop(): Unit = {
    log.debug(s"stopping spark source for '$queryId'")
    if (handle != null && handle.isAlive) {
      handle.destroyForcibly()
    }
  }

  override def preStart(): Unit = {
    log.debug(s"starting spark source '$queryId'")
  }

  //in case this publisher never gets subscribed to
  override def subscriptionTimeout: Duration = 1.minute

  val jobName = "sonicd-" + jar.replace(".jar", "") + "-" + queryId

  lazy val tmpJar: Try[URI] = Try {
    val tempFile = File.makeTemp(jobName, ".jar")
    val out = tempFile.bufferedOutput()
    try {
      val in = getClass.getClassLoader.getResourceAsStream(jar)
      if (in == null) {
        throw new Exception(s"missing '$jar' resource. " +
          s"available resources: ${
            val r = getClass.getClassLoader.getResources("")
            var buf = ""
            while (r.hasMoreElements) buf += r.nextElement()
            buf
          }")
      }
      IOUtils.copy(in, out)
      tempFile.jfile.toURI
    } finally {
      IOUtils.closeQuietly(out)
    }
  }

  lazy val env: Map[String, String] = Map(
    "HADOOP_CONF_DIR" → SonicdConfig.HADOOP_CONF.get,
    "YARN_CONF_DIR" → SonicdConfig.YARN_CONF.get,
    "SPARK_MASTER" → SonicdConfig.SPARK_MASTER.get,
    "QUERY" → query,
    "QUERY_ID" → queryId,
    "PRINT_COLUMN_NAMES" → printColNames.toString
  )

  case class Stderr(bytes: ByteString)

  case class Stdout(bytes: ByteString)

  var handle: Process = null
  var status: Future[Int] = null
  var stdout: Future[Unit] = null
  var stderr: Future[Unit] = null
  val buffer: ListBuffer[SonicMessage] = ListBuffer.empty
  var done: Boolean = false

  def stream(): Unit = {
    if (isActive && totalDemand > 0) {
      if (buffer.nonEmpty) {
        onNext(buffer.remove(0))
        stream()
      } else if (done) {
        onCompleteThenStop()
      } else {
        log.debug("buffer is empty '{}' and query is not done yet '{}", buffer, done)
      }
    }
  }

  def running(): Receive = {

    case Stdout(b) ⇒
      val out = SonicMessage.fromBytes(b)
      buffer.append(out)
      done = out.isInstanceOf[DoneWithQueryExecution]
      log.debug("STDOUT: " + out)
      stream()

    case Stderr(b) ⇒
      val str = b.decodeString("UTF-8")
      val out = QueryProgress(None, Some(str))
      buffer.append(out)
      log.debug("STDERR: " + out)
      stream()

    //FIXME race condition with stdout obj
    case d: DoneWithQueryExecution ⇒
      done = true
      buffer.append(d)
      stream()
      log.debug(s"received doneWithQueryExecution for '$queryId'")

    case Request(n) ⇒ stream()

    case Cancel ⇒
      log.info(s"client of '$queryId' canceled")
      onCompleteThenStop()

  }

  override def receive: Receive = {
    case SubscriptionTimeoutExceeded ⇒
      log.warning(s"no subscriber in within subs timeout $subscriptionTimeout")
      onCompleteThenStop()
    //first time client requests
    case Request(n) ⇒
      try {

        val jarUri = tmpJar.get.toString
        log.info(s"preparing launcher with jar '$jarUri'")

        implicit val mat = ActorMaterializer()

        handle = new SparkLauncher(env)
          //.setPropertiesFile(this.getClass.getClassLoader.getResource(SPARK_CONFIG).toURI.getPath)
          .setAppName(jobName)
          .setSparkHome(SonicdConfig.SPARK_HOME.get)
          .setAppResource(jarUri)
          .setMainClass(mainClass) //Main class in fat JAR
          .setMaster(SonicdConfig.SPARK_MASTER.get)
          //.setConf("spark.driver.memory", "2g")
          //.setConf("spark.yarn.queue", "root.imports")
          //.setConf("spark.akka.frameSize", "200")
          //.setConf("spark.executor.memory", "8g")
          //.setConf("spark.executor.instances", "8")
          //.setConf("spark.executor.cores", "12")
          //.setConf("spark.default.parallelism", "5000")
          .launch()

        //FIXME!
        //stderr = fromInputStream(handle.getErrorStream).runWith(Sink.foreach { s ⇒
        //  self ! Stderr(s)
        //})
        //stdout = fromInputStream(handle.getInputStream).runWith(Sink.foreach { s ⇒
        //  self ! Stdout(s)
        //})

        status = Future(handle.waitFor())

        status.andThen {
          case Success(s) ⇒ self ! DoneWithQueryExecution(success = s == 0)
          case Failure(e) ⇒ self ! DoneWithQueryExecution.error(e)
        }

        context.become(running())

      } catch {
        case e: Exception ⇒
          val msg = "there was an error when starting spark sql instance"
          log.error(e, msg)
          onNext(DoneWithQueryExecution.error(e))
          onCompleteThenStop()
      }
    case Cancel ⇒
      log.info(s"client of '$queryId' canceled")
      onCompleteThenStop()
  }
}
