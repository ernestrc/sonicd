package build.unstable.sonicd.source

import java.io.File
import java.nio.file.Path

import akka.actor._
import akka.stream.actor.ActorPublisher
import build.unstable.sonic.JsonProtocol._
import build.unstable.sonic.model.{Query, RequestContext, SonicMessage}
import build.unstable.sonicd.SonicdLogging
import build.unstable.sonicd.source.file.{FileWatcher, FileWatcherWorker, LocalFilePublisher}
import spray.json._

/**
  * Watches JSON files in 'path' local to Sonicd instance and exposes contents as a stream.
  *
  * Takes an optional 'tail' parameter to configure if only new data should be streamed.
  */
class LocalJsonStreamSource(query: Query, actorContext: ActorContext, context: RequestContext)
  extends SonicdSource(query, actorContext, context) {

  val publisher: Props = {
    val path = getConfig[String]("path")
    val tail = getOption[Boolean]("tail").getOrElse(true)

    val glob = FileWatcher.parseGlob(path)
    val workerProps = { dir: Path ⇒ Props(classOf[FileWatcherWorker], dir) }
    val watchers = LocalFilePublisher.getWatchers(glob, actorContext, workerProps)

    Props(classOf[LocalJsonPublisher], query.id.get, query.query, tail, glob.fileFilterMaybe, watchers, context)
  }
}

class LocalJsonPublisher(val queryId: Long,
                         val rawQuery: String,
                         val tail: Boolean,
                         val fileFilterMaybe: Option[String],
                         val watchersPair: Vector[(File, ActorRef)],
                         val ctx: RequestContext)
  extends Actor with ActorPublisher[SonicMessage] with SonicdLogging with LocalFilePublisher {

  override def parseUTF8Data(raw: String): Map[String, JsValue] =
    raw.parseJson.asJsObject.fields
}
