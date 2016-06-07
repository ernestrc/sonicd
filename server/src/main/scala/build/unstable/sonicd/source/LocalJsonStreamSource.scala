package build.unstable.sonicd.source

import java.io.RandomAccessFile
import java.nio.file._

import akka.actor.SupervisorStrategy.Stop
import akka.actor._
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import build.unstable.sonicd.model.JsonProtocol._
import build.unstable.sonicd.model._
import build.unstable.sonicd.source.FileWatcher.Watch
import spray.json._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.util.Try
import scala.util.control.NonFatal

/**
 * Watches JSON files in 'path' local to Sonicd instance and exposes contents as a stream.
 *
 * Takes an optional 'tail' parameter to configure if only new data should be streamed.
 */
class LocalJsonStreamSource(config: JsObject, queryId: String, query: String, context: ActorContext)
  extends DataSource(config, queryId, query, context) {

  val handlerProps: Props = {
    val folderPath = getConfig[String]("path")
    val tail = getOption[Boolean]("tail").getOrElse(false)

    Props(classOf[JsonStreamPublisher], queryId, folderPath, query, tail)
  }
}

class JsonStreamPublisher(queryId: String, folderPath: String, rawQuery: String, tail: Boolean)
  extends Actor with ActorPublisher[SonicMessage] with ActorLogging {

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    log.debug("starting json stream publisher of '{}'", queryId)
  }

  override def postStop(): Unit = {
    log.info(s"stopping json stream publisher of '$queryId'")
    //close resources
  }

  @throws[Exception](classOf[Exception])
  override def postRestart(reason: Throwable): Unit = {
    log.error(reason, "restarted json stream publisher")
  }

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy.apply(loggingEnabled = true) {
    case NonFatal(e) ⇒
      self ! DoneWithQueryExecution.error(e)
      log.error(e, "watcher terminated unexpectedly")
      Stop
  }

  //in case this publisher never gets subscribed to
  override def subscriptionTimeout: Duration = 10.seconds

  import context.dispatcher

  /* HELPERS */

  def newFile(fileName: String): RandomAccessFile = {
    val file = Paths.get(folderPath, fileName).toFile
    val reader = new RandomAccessFile(file, "r")

    val pos = if (tail) {
      val skip = file.length()
      reader.seek(skip)
      log.debug("tail mode enabled. skipping {} bytes", skip)
      skip
    } else 1L

    files.update(fileName, reader)
    reader
  }

  def terminate(done: DoneWithQueryExecution) = {
    onNext(done)
    onCompleteThenStop()
  }

  def filter(json: JsObject, query: Query): Option[Map[String, JsValue]] = {
    if (query.valueFilter(json)) {
      val fields = json.fields.filter(query.selectFilter)
      Some(fields)
    } else None
  }

  //first element dictates type metadata
  def metaFiltering(meta: Option[TypeMetadata], fields: Map[String, JsValue]): Vector[JsValue] = {
    meta match {
      case Some(m) ⇒ m.typesHint.map {
        case (s: String, v: JsValue) ⇒ fields.getOrElse(s, JsNull)
      }
      case None ⇒ fields.values.to[Vector]
    }
  }

  @tailrec
  final def stream(fileName: String, reader: RandomAccessFile, query: Query): Unit = {
    lazy val read = {
      if (buffer.isEmpty) {
        Option(reader.readLine())
      }
      else Option(buffer.remove(0))
    }

    lazy val raw = read.get
    lazy val json = Try(raw.parseJson.asJsObject)

    if (totalDemand > 0 && read.isDefined && json.isSuccess) {

      val filtered = filter(json.get, query)

      if (meta.isEmpty && filtered.isDefined) {

        meta = query.select.map { select ⇒
          //FIXME select types potentially not known at this point
          TypeMetadata(select.map(s ⇒ s → filtered.get.getOrElse(s, JsNull)))
        }.orElse {
          Some(TypeMetadata(filtered.get.toVector))
        }

        onNext(meta.get)
      }

      filtered match {
        case Some(fields) if totalDemand > 0 ⇒
          onNext(OutputChunk(JsArray(metaFiltering(meta, fields))))
        case Some(fields) ⇒ buffer.append(raw)
        case None ⇒
      }

      stream(fileName, reader, query)

    } else if (totalDemand > 0 && read.isDefined && json.isFailure) {

      stream(fileName, reader, query)

    }
  }

  case class Query(raw: String,
                   selectFilter: ((String, JsValue)) ⇒ Boolean,
                   select: Option[Vector[String]],
                   valueFilter: JsObject ⇒ Boolean)

  def matchObject(filter: JsObject): JsObject ⇒ Boolean = (o: JsObject) ⇒ {
    val filterFields = filter.fields
    val oFields = o.fields
    filterFields.forall {
      case (key, j: JsObject) if oFields.isDefinedAt(key) && oFields(key).isInstanceOf[JsObject] ⇒
        matchObject(j)(oFields(key).asInstanceOf[JsObject])
      case (key, value) ⇒ oFields.isDefinedAt(key) && oFields(key) == value
    }
  }

  /**
   * {
   * "select" : ["field1", "field2"],
   * "filter" : { "field1" : "value1" }
   * }
   */
  def parseQuery(raw: String): Query = {
    val r = raw.parseJson.asJsObject(s"Query must be a valid JSON object: $raw").fields

    val select = r.get("select").map { v ⇒
      val fields = v.convertTo[Vector[String]]
      ((v: (String, JsValue)) ⇒ fields.contains(v._1)) → Some(fields)
    }.getOrElse(((v: (String, JsValue)) ⇒ true) → None)

    val f = r.get("filter").map { fo ⇒
      val fObj = fo.asJsObject(s"filter key must be a valid JSON object: ${fo.compactPrint}")
      matchObject(fObj)
    }.getOrElse((o: JsObject) ⇒ true)

    Query(raw, select._1, select._2, f)
  }


  /* STATE */

  val files = mutable.Map.empty[String, RandomAccessFile]
  val buffer = ListBuffer.empty[String]
  var meta: Option[TypeMetadata] = None
  val watching: Boolean = false

  /* BEHAVIOUR */

  def streaming(query: Query): Receive = {

    case req: Request ⇒
      files.foreach(kv ⇒ stream(kv._1, kv._2, query))

    case done: DoneWithQueryExecution ⇒
      if (totalDemand > 0) terminate(done)
      else context.system.scheduler.scheduleOnce(1.second, self, done)

    case event: WatchEvent[_] ⇒
      val kind = event.kind()

      //not registered OVERFLOW so all events context are Path
      val ev = event.asInstanceOf[WatchEvent[Path]]
      val fileName = ev.context().toFile.toString

      //log.debug("file {} changed: {}", fileName, kind.name())

      kind match {
        case StandardWatchEventKinds.ENTRY_CREATE ⇒ newFile(fileName)
        case StandardWatchEventKinds.ENTRY_MODIFY ⇒
          files.get(fileName) match {
            case Some(file) ⇒
              stream(fileName, file, query)
            case None ⇒
              val file = newFile(fileName)
              stream(fileName, file, query)
          }
        case StandardWatchEventKinds.ENTRY_DELETE ⇒ files.remove(fileName)
      }
  }

  def receive: Receive = {
    case req: Request ⇒
      log.debug("starting query {} watch on path {}", rawQuery, folderPath)

      try {
        val parsed = parseQuery(rawQuery)
        for (file <- new java.io.File(folderPath).listFiles) {
          if (file.isFile) {
            val fileName = file.getName
            val reader = newFile(fileName)
            files.update(fileName, reader)
          }
        }

        val fw = context.actorOf(Props(classOf[FileWatcher], Paths.get(folderPath), queryId))
        fw ! FileWatcher.Watch
        self ! req
        context.become(streaming(parsed))

      } catch {
        case e: Exception ⇒
          terminate(DoneWithQueryExecution.error(e))
      }

    case Cancel ⇒
      log.debug("client canceled")
      onCompleteThenStop()
  }
}

object FileWatcher {

  case object Watch

}

class FileWatcher(folder: Path, queryId: String) extends Actor with ActorLogging {

  import java.nio.file._

  import scala.collection.JavaConversions._

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    log.debug("starting file watcher of '{}'", queryId)
  }

  override def postStop(): Unit = {
    log.info(s"stopping file watcher of '$queryId'")
    try {
      watcher.close()
      key.cancel()
      log.debug("closed watcher object {}", watcher)
    } catch {
      case e: Exception ⇒
    }
  }

  var watcher: WatchService = FileSystems.getDefault.newWatchService
  log.debug("created watcher object {}", watcher)

  def watch(): List[WatchEvent[_]] = {
    val k = watcher.take
    val events = k.pollEvents

    if (k.reset()) {
      events.toList
    } else throw new Exception("aborted")
  }

  val key = folder.register(
    watcher,
    StandardWatchEventKinds.ENTRY_CREATE,
    StandardWatchEventKinds.ENTRY_MODIFY,
    StandardWatchEventKinds.ENTRY_DELETE
  )

  override def receive: Actor.Receive = {
    case Watch ⇒
      log.debug("watching contents of folder {}", folder)
      val ev = watch()
      if (ev.nonEmpty) {
        ev.foreach(context.parent ! _)
      }

      self ! Watch
    case msg ⇒ log.warning("oops! extraneous message")
  }
}
