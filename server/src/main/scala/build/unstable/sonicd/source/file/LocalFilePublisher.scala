package build.unstable.sonicd.source.file

import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.file._

import akka.actor._
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import akka.util.ByteString
import build.unstable.sonic.model._
import build.unstable.sonicd.SonicdLogging
import build.unstable.sonicd.source.IncrementalMetadataSupport
import build.unstable.sonicd.source.file.FileWatcher.{Glob, PathWatchEvent}
import build.unstable.sonicd.source.file.LocalFilePublisher.BufferedFileByteChannel
import spray.json._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.Try

/**
 * Watches files in 'path' local to Sonicd instance and exposes contents as a stream.
 * Subclasses need to implement `parseUTF8Data`
 */
trait LocalFilePublisher extends IncrementalMetadataSupport {
  this: Actor with ActorPublisher[SonicMessage] with SonicdLogging ⇒

  import build.unstable.sonicd.source.json.JsonUtils._


  /* ABSTRACT */

  def parseUTF8Data(raw: String): JsValue

  def rawQuery: String

  def tail: Boolean

  def fileFilterMaybe: Option[String]

  def watchersPair: Vector[(File, ActorRef)]

  def ctx: RequestContext


  /* OVERRIDES */

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    log.debug("starting file stream publisher of '{}'", ctx.traceId)
  }

  override def postStop(): Unit = {
    log.debug("stopping file stream publisher of '{}'", ctx.traceId)
  }

  @throws[Exception](classOf[Exception])
  override def postRestart(reason: Throwable): Unit = {
    log.error(reason, "restarted file stream publisher")
  }

  //in case this publisher never gets subscribed to
  override def subscriptionTimeout: Duration = 10.seconds


  /* HELPERS */

  def tryPushDownstream() {
    while (isActive && totalDemand > 0 && buffer.nonEmpty) {
      onNext(buffer.dequeue())
    }
  }

  def newFile(file: File, fileName: String): Option[BufferedFileByteChannel] = try {

    val channel = BufferedFileByteChannel(
      Files.newByteChannel(file.toPath, StandardOpenOption.READ),
      file.getAbsoluteFile.getName)

    if (tail) {
      val bytes = file.length()
      log.debug("tail mode enabled. skipping {} bytes for {}", bytes, fileName)
      channel.channel.position(bytes)
    }

    files.update(fileName, file → channel)
    Some(channel)
  } catch {
    case e: AccessDeniedException ⇒
      log.warning("access denied on {}", fileName)
      None
  }

  /**
   * streams until demand is 0 or there is no more data to be read
   *
   * @return whether there is no more data to read
   */
  @tailrec
  final def stream(query: ParsedQuery,
                   channel: BufferedFileByteChannel): Boolean = {

    lazy val read = channel.readLine()
    lazy val raw = read.get
    lazy val data = Try(parseUTF8Data(raw))

    tryPushDownstream()

    if (totalDemand > 0 && read.nonEmpty && data.isSuccess) {
      bufferNext(query, data.get)
      stream(query, channel)
    } else if /* problem parsing the data */(totalDemand > 0 && read.nonEmpty) stream(query, channel)
    else {
      // if totalDemand is 0, then read must not be applied
      // or we will skip the line
      totalDemand == 0
    }
  }


  /* STATE */

  val (folders, watchers) = watchersPair.unzip
  val files = mutable.Map.empty[String, (File, BufferedFileByteChannel)]
  val buffer: mutable.Queue[SonicMessage] = mutable.Queue(StreamStarted(ctx.traceId))
  val watching: Boolean = false


  /* BEHAVIOUR */

  def terminating(done: StreamCompleted): Receive = {
    tryPushDownstream()
    if (buffer.isEmpty && isActive && totalDemand > 0) {
      onNext(done)
      onCompleteThenStop()
    } else if (!isActive) context.stop(self)

    {
      case r: Request ⇒ terminating(done)
    }
  }

  final def common: Receive = {
    case Cancel ⇒
      log.debug("client canceled")
      onComplete()
      context.stop(self)
  }

  final def streaming(query: ParsedQuery, totalInitialFiles: Int): Receive = common orElse {

    case req: Request ⇒
      var totalDataLeft = false
      files.clone().foreach { file ⇒
        val dataLeft = stream(query, file._2._2)
        if (!dataLeft && !tail) {
          buffer.enqueue(QueryProgress(QueryProgress.Running, 1, Some(totalInitialFiles), Some("files")))
          files.remove(file._1)
          log.debug("finished scanning {}", file._1)
        }
        totalDataLeft ||= dataLeft
      }
      if (!tail && !totalDataLeft) context.become(terminating(StreamCompleted.success(ctx)))

    case done: StreamCompleted ⇒ context.become(terminating(done))

    case ev@PathWatchEvent(_, event) ⇒
      event.kind() match {

        case StandardWatchEventKinds.ENTRY_CREATE ⇒
          newFile(ev.file, ev.fileName).foreach { f ⇒
            files.update(ev.fileName, ev.file → f)
          }

        case StandardWatchEventKinds.ENTRY_DELETE ⇒
          files.remove(ev.fileName)

        case StandardWatchEventKinds.ENTRY_MODIFY ⇒
          files.get(ev.fileName).map {
            case (_, p) ⇒ p
          }.orElse {
            log.debug("file {} was modified and but it was not being monitored yet", ev.fileName)
            newFile(ev.file, ev.fileName)
          }.foreach { chan ⇒
            stream(query, chan)
          }
      }

    case anyElse ⇒ log.warning("extraneous message {}", anyElse)
  }

  final def receive: Receive = common orElse {
    case req: Request ⇒
      log.debug("running file query {}", rawQuery)

      try {
        val parsed = parseQuery(rawQuery)

        val totalInitialFiles = folders.foldLeft(0) { (facc, folder) ⇒
          val list = folder.listFiles()
          if (list != null) facc + list.foldLeft(0) { (acc, file) ⇒

            lazy val matcher = fileFilterMaybe.map(fileFilter ⇒
              FileSystems.getDefault.getPathMatcher(s"glob:${folder.toString + "/" + fileFilter}")
            )
            if (file.isFile && (matcher.isEmpty || matcher.get.matches(file.toPath))) {
              log.debug("matched file {}", file.toPath)
              val fileName = file.getName
              newFile(Paths.get(folder.toPath.toString, fileName).toFile, fileName)
                .map { reader ⇒
                  files.update(fileName, file → reader)
                  acc + 1
                }.getOrElse(acc)
            } else acc
          } else facc
        }
        log.info("matched {} initial files", totalInitialFiles)

        //watch only if we're tailing logs
        if (tail) watchers.foreach(_ ! FileWatcher.Watch(fileFilterMaybe, ctx))
        self ! req
        context.become(streaming(parsed, totalInitialFiles))

      } catch {
        case e: Exception ⇒
          log.error(e, "error setting up watch")
          context.become(terminating(StreamCompleted.error(ctx.traceId, e)))
      }
    case anyElse ⇒ log.warning("extraneous message {}", anyElse)
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
      while ( {
        char = readChar(); char.isDefined
      } && char.get != '\n' && char.get != '\r') {
        builder append char.get
      }

      val string = builder.toString()

      //none signals end of file
      if (string.isEmpty && channel.position() >= channel.size()) None
      else Some(string)
    }
  }

  def getWatchers(glob: Glob, context: ActorContext, workerProps: Path ⇒ Props): Vector[(File, ActorRef)] = {
    glob.folders.map(f ⇒ f.toFile → LocalFilePublisher.getWatcher(f, context, workerProps(f))).to[Vector]
  }

  def getWatcher(path: Path, context: ActorContext, workerProps: Props): ActorRef = {
    context.child(path.toString).getOrElse {
      context.actorOf(Props(classOf[FileWatcher], path,
        workerProps.withDispatcher(FileWatcher.dispatcher))
        .withDispatcher(FileWatcher.dispatcher))
    }
  }
}
