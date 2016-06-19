package build.unstable.sonicd.source.file

import java.io.File
import java.nio.file._

import akka.actor._
import build.unstable.sonicd.model._
import build.unstable.sonicd.source.file.FileWatcher.{PathWatchEvent, Watch}
import build.unstable.sonicd.source.file.FileWatcherWorker.DoWatch

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.mutable

class FileWatcher(dir: Path) extends Actor with SonicdLogging {

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    debug(log, "starting file watcher of folder {}", dir)
  }

  override def postStop(): Unit = {
    debug(log, "stopping file watcher of '{}'", dir)
  }

  val subscribers = mutable.Map.empty[ActorRef, Option[PathMatcher]]
  val worker: ActorRef = context.actorOf(Props(classOf[FileWatcherWorker], dir))
  worker ! DoWatch

  override def receive: Actor.Receive = {

    //not registered OVERFLOW so all events context are Path
    case ev: WatchEvent[Path]@unchecked ⇒
      val event = PathWatchEvent(dir, ev)

      subscribers.foreach {
        case (sub, Some(filter)) if event.matches(filter) ⇒ sub ! event
        case (sub, None) ⇒ sub ! event
        case _ ⇒ debug(log, "ev ignored: {}", ev)
      }

      sender() ! FileWatcherWorker.DoWatch

    case Watch(Some(fileFilter)) ⇒
      val filter = s"glob:${dir.toString + "/" + fileFilter}"
      debug(log, "received watch directive with filter {}", filter)
      val matcher = FileSystems.getDefault.getPathMatcher(filter)
      subscribers.update(sender(), Some(matcher))
    case Watch(None) ⇒
      subscribers.update(sender(), None)

    case msg ⇒ warning(log, "extraneous message received {}", msg)
  }
}

object FileWatcher extends SonicdLogging {

  case class PathWatchEvent(dir: Path, event: WatchEvent[Path]) {
    //relative path
    private val $file = event.context().toFile
    val fileName = $file.toString
    //build absolute path
    val path = Paths.get(dir.toString, fileName)
    val file = path.toFile

    def matches(filter: PathMatcher) = {
      filter.matches(path)
    }
  }

  case class Watch(fileFilterMaybe: Option[String])

  case class Glob(folders: Set[Path], fileFilterMaybe: Option[String]) {
    def isEmpty: Boolean = folders.isEmpty
  }

  object Glob {
    val empty: Glob = Glob(Set.empty, None)
  }

  //FIXME check FileWatcherSpec tests
  @tailrec
  private final def doParse(fps: Vector[File],
                            recursive: Boolean = false,
                            res: Glob = Glob.empty,
                            dirFilter: Path ⇒ Boolean = (p: Path) ⇒ true): Glob =
    fps.headOption match {
      case Some(fp) ⇒
        debug(log, "expanding file path {} ", fp)
        val path = fp.getPath

        lazy val newGlob = Glob(res.folders + fp.toPath, res.fileFilterMaybe)

        if (fp.isDirectory && recursive) {
          doParse(fps.tail ++ fp.listFiles().filter(_.isDirectory).toVector, recursive, newGlob, dirFilter)
        } else if (fp.isDirectory) {
          newGlob
        } else {
          val segments = path.split("/")
          val (rest, last) = segments.splitAt(segments.length - 1)
          lazy val restPath = new File(rest.reduce(_ + "/" + _))

          if (last.head == "**") {
            doParse(fps.tail :+ restPath, recursive = true, Glob(res.folders, res.fileFilterMaybe), dirFilter)
          } else if (last.head.contains("*") || fp.isFile || path.contains("**")) {
            //if last segment contains wildcard or
            //path is valid file or
            doParse(fps.tail :+ restPath, recursive, Glob(res.folders, Some(last.head)), dirFilter)
          } else doParse(fps.tail, recursive, res, dirFilter)
        }
      case None ⇒ res.copy(folders = res.folders.filter(dirFilter))
    }

  def parseGlob(raw: String): Glob = {
    assert(raw.nonEmpty, "path can't be empty!")
    val glob = doParse(Vector(new File(raw)))
    assert(!glob.isEmpty, s"$raw matched no directories")
    glob
  }
}

class FileWatcherWorker(dir: Path) extends Actor with SonicdLogging {
  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    debug(log, "starting file watcher worker of folder {}", dir)
  }

  override def postStop(): Unit = {
    debug(log, "stopping file watcher worker of '{}'", dir)
    try {
      watcher.close()
      key.cancel()
      debug(log, "closed watcher object {}", watcher)
    } catch {
      case e: Exception ⇒
    }
  }

  val watcher: WatchService = FileSystems.getDefault.newWatchService()

  val key = dir.register(
    watcher,
    StandardWatchEventKinds.ENTRY_CREATE,
    StandardWatchEventKinds.ENTRY_MODIFY,
    StandardWatchEventKinds.ENTRY_DELETE
  )

  def watch(): List[WatchEvent[_]] = {
    val k = watcher.take()
    val events = k.pollEvents()

    if (k.reset()) {
      events.toList
    } else throw new Exception("aborted")
  }

  override def receive: Actor.Receive = {
    case DoWatch ⇒
      val ev = watch()
      if (ev.nonEmpty) ev.foreach(e ⇒ context.parent ! e)
      else self ! DoWatch
    case msg ⇒ warning(log, "extraneous message received {}", msg)
  }
}

object FileWatcherWorker {

  case object DoWatch

}
