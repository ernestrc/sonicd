package build.unstable.sonicd.source

import java.io.File

import akka.actor.{Actor, ActorContext, ActorRef, Props}
import akka.stream.actor.ActorPublisher
import build.unstable.sonicd.model.JsonProtocol._
import build.unstable.sonicd.model._
import build.unstable.sonicd.source.file.{FileWatcher, LocalFilePublisher}
import spray.json._

/**
 * Watches JSON files in 'path' local to Sonicd instance and exposes contents as a stream.
 *
 * Takes an optional 'tail' parameter to configure if only new data should be streamed.
 */
class RawFileStreamSource(query: Query, actorContext: ActorContext, context: RequestContext)
  extends DataSource(query, actorContext, context) {

  val handlerProps: Props = {
    val path = getConfig[String]("path")
    val tail = getOption[Boolean]("tail").getOrElse(true)

    val glob = FileWatcher.parseGlob(path)
    val watchers = LocalFilePublisher.getWatchers(glob, actorContext)

    Props(classOf[LocalRawFilePublisher], query.id.get, query.query, tail, glob.fileFilterMaybe, watchers, context)

  }
}

class LocalRawFilePublisher(val queryId: Long,
                            val rawQuery: String,
                            val tail: Boolean,
                            val fileFilterMaybe: Option[String],
                            val watchersPair: Vector[(File, ActorRef)],
                            val ctx: RequestContext)
  extends Actor with ActorPublisher[SonicMessage] with SonicdLogging with LocalFilePublisher {

  override def parseUTF8Data(raw: String): Map[String, JsValue] =
    Map("raw" → JsString(raw))

}
