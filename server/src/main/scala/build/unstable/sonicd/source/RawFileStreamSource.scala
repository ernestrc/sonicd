package build.unstable.sonicd.source

import java.io.File

import akka.actor.{Actor, ActorContext, ActorRef, Props}
import akka.stream.actor.ActorPublisher
import build.unstable.sonicd.auth.ApiUser
import build.unstable.sonicd.model.{JsonProtocol, DataSource, SonicMessage, SonicdLogging}
import build.unstable.sonicd.source.file.{FileWatcher, LocalFilePublisher}
import spray.json._
import JsonProtocol._

/**
 * Watches JSON files in 'path' local to Sonicd instance and exposes contents as a stream.
 *
 * Takes an optional 'tail' parameter to configure if only new data should be streamed.
 */
class RawFileStreamSource(config: JsObject, queryId: String, query: String, context: ActorContext, apiUser: Option[ApiUser])
  extends DataSource(config, queryId, query, context, apiUser) {

  val handlerProps: Props = {
    val path = getConfig[String]("path")
    val tail = getOption[Boolean]("tail").getOrElse(true)

    val glob = FileWatcher.parseGlob(path)
    val watchers = LocalFilePublisher.getWatchers(glob, context)

    Props(classOf[LocalRawFilePublisher], queryId,
      query, tail, glob.fileFilterMaybe, watchers)

  }
}

class LocalRawFilePublisher(val queryId: String,
                            val rawQuery: String,
                            val tail: Boolean,
                            val fileFilterMaybe: Option[String],
                            val watchersPair: Vector[(File, ActorRef)])
  extends Actor with ActorPublisher[SonicMessage] with SonicdLogging with LocalFilePublisher {

  override def parseUTF8Data(raw: String): Map[String, JsValue] =
    Map("raw" → JsString(raw))

}
