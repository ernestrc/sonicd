package build.unstable.sonicd.source.file

import java.io.{File, RandomAccessFile}
import java.nio.file._

import akka.actor._
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import build.unstable.sonicd.model.JsonProtocol._
import build.unstable.sonicd.model._
import build.unstable.sonicd.source.file.FileWatcher.{Glob, PathWatchEvent}
import spray.json._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.util.Try

/**
 * Watches files in 'path' local to Sonicd instance and exposes contents as a stream.
 * Subclasses need to implement `parseUTF8Data`
 */
trait LocalFilePublisher {
  this: Actor with ActorPublisher[SonicMessage] with SonicdLogging ⇒

  import context.dispatcher


  /* ABSTRACT */

  def parseUTF8Data(raw: String): Map[String, JsValue]

  val queryId: String
  val rawQuery: String
  val tail: Boolean
  val fileFilterMaybe: Option[String]
  val watchersPair: Vector[(File, ActorRef)]


  /* OVERRIDES */

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    debug(log, "starting file stream publisher of '{}'", queryId)
  }

  override def postStop(): Unit = {
    debug(log, "stopping file stream publisher of '{}'", queryId)
  }

  @throws[Exception](classOf[Exception])
  override def postRestart(reason: Throwable): Unit = {
    error(log, reason, "restarted file stream publisher")
  }

  //in case this publisher never gets subscribed to
  override def subscriptionTimeout: Duration = 10.seconds


  case class FileQuery(raw: String,
                       select: Option[Vector[String]],
                       valueFilter: Map[String, JsValue] ⇒ Boolean)

  /* HELPERS */

  def terminate(done: DoneWithQueryExecution) = {
    onNext(done)
    onCompleteThenStop()
  }

  def newFile(file: File, fileName: String): RandomAccessFile = {
    val reader = new RandomAccessFile(file, "r")

    if (tail) {
      val skip = file.length()
      reader.seek(skip)
      debug(log, "tail mode enabled. skipping {} bytes for {}", skip, fileName)
    }

    files.update(fileName, reader)
    reader
  }

  def matchObject(filter: Map[String, JsValue]): Map[String, JsValue] ⇒ Boolean = (o: Map[String, JsValue]) ⇒ {
    val filterFields = filter
    val oFields = o
    filterFields.forall {
      case (key, j: JsObject) if oFields.isDefinedAt(key) && oFields(key).isInstanceOf[JsObject] ⇒
        matchObject(j.fields)(oFields(key).asInstanceOf[JsObject].fields)
      case (key, value) ⇒ oFields.isDefinedAt(key) && oFields(key) == value
    }
  }

  /**
   * {
   * "select" : ["field1", "field2"],
   * "filter" : { "field1" : "value1" }
   * }
   */
  def parseQuery(raw: String): FileQuery = {
    val r = raw.parseJson.asJsObject(s"Query must be a valid JSON object: $raw").fields

    val select = r.get("select").map { v ⇒
      v.convertTo[Vector[String]]
    }

    val f = r.get("filter").map { fo ⇒
      val fObj = fo.asJsObject(s"filter key must be a valid JSON object: ${fo.compactPrint}").fields
      matchObject(fObj)
    }.getOrElse((o: Map[String, JsValue]) ⇒ true)

    FileQuery(raw, select, f)
  }

  def select(meta: Option[TypeMetadata], fields: Map[String, JsValue]): Vector[JsValue] = {
    meta match {
      case Some(m) ⇒ m.typesHint.map {
        case (s: String, v: JsValue) ⇒ fields.getOrElse(s, JsNull)
      }
      case None ⇒ fields.values.to[Vector]
    }
  }

  def filter(data: Map[String, JsValue], query: FileQuery): Option[Map[String, JsValue]] = {
    if (query.valueFilter(data)) {
      if (query.select.isEmpty) Some(data)
      else {
        val fields = data.filter(kv ⇒ query.select.get.contains(kv._1))
        Some(fields)
      }
    } else None
  }

  @tailrec
  final def stream(reader: RandomAccessFile, query: FileQuery): Unit = {
    lazy val read = {
      if (buffer.isEmpty) {
        Option(reader.readLine())
      }
      else Option(buffer.remove(0))
    }

    lazy val raw = read.get
    lazy val data = Try(parseUTF8Data(raw))

    if (totalDemand > 0 && read.isDefined && data.isSuccess) {

      val filteredMaybe = filter(data.get, query)

      if (meta.isEmpty && filteredMaybe.isDefined) {

        meta = query.select.map { select ⇒
          //FIXME select types potentially not known at this point
          TypeMetadata(select.map(s ⇒ s → filteredMaybe.get.getOrElse(s, JsNull)))
        }.orElse {
          Some(TypeMetadata(filteredMaybe.get.toVector))
        }

        onNext(meta.get)
      }

      filteredMaybe match {
        case Some(filtered) if totalDemand > 0 ⇒ onNext(OutputChunk(JsArray(select(meta, filtered))))
        case Some(fields) ⇒ buffer.append(raw)
        case None ⇒
      }

      stream(reader, query)

    } else if (totalDemand > 0 && read.isDefined && data.isFailure) {

      stream(reader, query)

    }
  }


  /* STATE */

  val (folders, watchers) = watchersPair.unzip
  val files = mutable.Map.empty[String, RandomAccessFile]
  val buffer = ListBuffer.empty[String]
  var meta: Option[TypeMetadata] = None
  val watching: Boolean = false


  /* BEHAVIOUR */

  final def streaming(query: FileQuery): Receive = {

    case req: Request ⇒
      files.foreach(kv ⇒ stream(kv._2, query))

    case done: DoneWithQueryExecution ⇒
      if (totalDemand > 0) terminate(done)
      else context.system.scheduler.scheduleOnce(1.second, self, done)

    case ev@PathWatchEvent(dir, event) ⇒
      val kind = event.kind()

      kind match {
        case StandardWatchEventKinds.ENTRY_CREATE ⇒ newFile(ev.file, ev.fileName)
        case StandardWatchEventKinds.ENTRY_MODIFY ⇒
          files.get(ev.fileName) match {
            case Some(file) ⇒
              stream(file, query)
            case None ⇒
              val file = newFile(ev.file, ev.fileName)
              stream(file, query)
          }
        case StandardWatchEventKinds.ENTRY_DELETE ⇒
          files.remove(ev.fileName)
      }
  }

  final def receive: Receive = {
    case req: Request ⇒
      debug(log, "running query {}", rawQuery)

      try {
        val parsed = parseQuery(rawQuery)

        folders.foreach { folder ⇒
          folder.listFiles.foreach { file ⇒

            lazy val matcher = fileFilterMaybe.map(fileFilter ⇒
              FileSystems.getDefault.getPathMatcher(s"glob:${folder.toString + "/" + fileFilter}")
            )
            if (file.isFile && (matcher.isEmpty || matcher.get.matches(file.toPath))) {
              val fileName = file.getName
              val reader = newFile(Paths.get(folder.toPath.toString, fileName).toFile, fileName)
              files.update(fileName, reader)
            }
          }
        }

        watchers.foreach(_ ! FileWatcher.Watch(fileFilterMaybe))
        self ! req
        context.become(streaming(parsed))

      } catch {
        case e: Exception ⇒
          error(log, e, "error setting up watch")
          terminate(DoneWithQueryExecution.error(e))
      }

    case Cancel ⇒
      debug(log, "client canceled")
      onCompleteThenStop()
  }
}

object LocalFilePublisher {

  def getWatchers(glob: Glob, context: ActorContext): Vector[(File, ActorRef)] = {
    glob.folders.map(f ⇒ f.toFile → LocalFilePublisher.getWatcher(f, context)).to[Vector]
  }

  def getWatcher(path: Path, context: ActorContext): ActorRef = {
    context.child(path.toString).getOrElse {
      context.actorOf(Props(classOf[FileWatcher], path))
    }
  }
}
