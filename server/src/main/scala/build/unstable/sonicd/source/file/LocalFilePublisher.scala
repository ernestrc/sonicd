package build.unstable.sonicd.source.file

import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.file._

import akka.actor._
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import akka.util.ByteString
import build.unstable.sonicd.model.JsonProtocol._
import build.unstable.sonicd.model._
import build.unstable.sonicd.source.file.FileWatcher.{Glob, PathWatchEvent}
import build.unstable.sonicd.source.file.LocalFilePublisher.BufferedFileByteChannel
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

  def queryId: Long

  def rawQuery: String

  def tail: Boolean

  def fileFilterMaybe: Option[String]

  def watchersPair: Vector[(File, ActorRef)]

  def ctx: RequestContext


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

  def newFile(file: File, fileName: String): BufferedFileByteChannel = {

    val channel = BufferedFileByteChannel(
      Files.newByteChannel(file.toPath, StandardOpenOption.READ),
      file.getAbsoluteFile.getName)

    if (tail) {
      val bytes = file.length()
      debug(log, "tail mode enabled. skipping {} bytes for {}", bytes, fileName)
      channel.channel.position(bytes)
    }

    files.update(fileName, file → channel)
    channel
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

  def filter(data: Map[String, JsValue], query: FileQuery, target: String): Option[Map[String, JsValue]] = {
    if (query.valueFilter(data) && target != "application.conf" && target != "reference.conf") {
      if (query.select.isEmpty) Some(data)
      else {
        val fields = data.filter(kv ⇒ query.select.get.contains(kv._1))
        Some(fields)
      }
    } else None
  }

  /**
   * streams until demand is 0 or there is no more data to be read
   *
   * @return whether there is no more data to read
   */
  @tailrec
  final def stream(query: FileQuery,
                   channel: BufferedFileByteChannel): Boolean = {

    lazy val read = {
      if (buffer.isEmpty) {
        channel.readLine()
      } else Option(buffer.remove(0))
    }

    lazy val raw = read.get
    lazy val data = Try(parseUTF8Data(raw))

    if (totalDemand > 0 && read.nonEmpty && data.isSuccess) {
      val filteredMaybe = filter(data.get, query, channel.fileName)

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

      stream(query, channel)

      //problem parsing the data
    } else if (totalDemand > 0 && read.nonEmpty) {
      warning(log, "error parsing UTF-8 data {}: {}", raw, data.failed.get.getMessage)
      stream(query, channel)
    } else {
      totalDemand == 0
    }
  }


  /* STATE */

  val (folders, watchers) = watchersPair.unzip
  val files = mutable.Map.empty[String, (File, BufferedFileByteChannel)]
  val buffer = ListBuffer.empty[String]
  var meta: Option[TypeMetadata] = None
  val watching: Boolean = false


  /* BEHAVIOUR */

  final def common: Receive = {
    case Cancel ⇒
      debug(log, "client canceled")
      onCompleteThenStop()
  }

  final def streaming(query: FileQuery): Receive = common orElse {

    case req: Request ⇒
      val dataLeft = files.nonEmpty && files.forall(kv ⇒ stream(query, kv._2._2))
      if (!tail && !dataLeft) terminate(DoneWithQueryExecution.success)

    case done: DoneWithQueryExecution ⇒
      if (totalDemand > 0) terminate(done)
      else context.system.scheduler.scheduleOnce(100.millis, self, done)

    case ev@PathWatchEvent(_, event) ⇒

      val channel = files.get(ev.fileName) match {
        case Some((_, p)) ⇒ p
        case None ⇒
          debug(log, "file {} was modified and but it was not being consumed yet", ev.fileName)
          newFile(ev.file, ev.fileName)
      }
      stream(query, channel)

    case anyElse ⇒ warning(log, "extraneous message {}", anyElse)
  }

  final def receive: Receive = common orElse {
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
              info(log, "matched file {}", file.toPath)
              val fileName = file.getName
              val reader = newFile(Paths.get(folder.toPath.toString, fileName).toFile, fileName)
              files.update(fileName, file → reader)
            }
          }
        }

        //watch only if we're tailing logs
        if (tail) watchers.foreach(_ ! FileWatcher.Watch(fileFilterMaybe, queryId))
        self ! req
        context.become(streaming(parsed))

      } catch {
        case e: Exception ⇒
          error(log, e, "error setting up watch")
          terminate(DoneWithQueryExecution.error(e))
      }
    case anyElse ⇒ warning(log, "extraneous message {}", anyElse)
  }
}

object LocalFilePublisher {

  //1 Kb
  val CHUNK = 1000

  case class BufferedFileByteChannel(channel: SeekableByteChannel, fileName: String) {
    val buf: ByteBuffer = ByteBuffer.allocateDirect(CHUNK)
    var stringBuf = scala.collection.mutable.Buffer.empty[Char]

    def readChar(): Option[Char] = {
      if (stringBuf.isEmpty) {
        channel.read(buf)
        buf.flip()
        stringBuf ++= ByteString(buf).utf8String
        buf.compact()
      }

      if (stringBuf.isEmpty) None
      else Some(stringBuf.remove(0))
    }

    /**
     * Returns None if reached EOF
     */
    def readLine(): Option[String] = {
      val builder = mutable.StringBuilder.newBuilder
      var char: Option[Char] = None
      while ({ char = readChar(); char.isDefined } && char.get != '\n' && char.get != '\r') {
        builder append char.get
      }

      val string = builder.toString()

      //none signals end of file
      if (string.isEmpty && channel.position() >= channel.size()) None
      else Some(string)
    }
  }

  def getWatchers(glob: Glob, context: ActorContext): Vector[(File, ActorRef)] = {
    glob.folders.map(f ⇒ f.toFile → LocalFilePublisher.getWatcher(f, context)).to[Vector]
  }

  def getWatcher(path: Path, context: ActorContext): ActorRef = {
    context.child(path.toString).getOrElse {
      context.actorOf(Props(classOf[FileWatcher], path)
        .withDispatcher(FileWatcher.dispatcher))
    }
  }
}
