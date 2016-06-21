package build.unstable.sonicd.source

import java.io.File

import akka.actor._
import akka.stream.actor.ActorPublisher
import build.unstable.sonicd.auth.ApiUser
import build.unstable.sonicd.model.JsonProtocol._
import build.unstable.sonicd.model._
import build.unstable.sonicd.source.file.{FileWatcher, LocalFilePublisher}
import spray.json._

/**
 * Watches JSON files in 'path' local to Sonicd instance and exposes contents as a stream.
 *
 * Takes an optional 'tail' parameter to configure if only new data should be streamed.
 */
class LocalJsonStreamSource(config: JsObject, queryId: String,
                            query: String, context: ActorContext, apiUser: Option[ApiUser])
  extends DataSource(config, queryId, query, context, apiUser) {

  val handlerProps: Props = {
    val path = getConfig[String]("path")
    val tail = getOption[Boolean]("tail").getOrElse(true)

    val glob = FileWatcher.parseGlob(path)
    val watchers = LocalFilePublisher.getWatchers(glob, context)

    Props(classOf[LocalJsonPublisher], queryId,
      query, tail, glob.fileFilterMaybe, watchers)
  }
}

class LocalJsonPublisher(val queryId: String,
                         val rawQuery: String,
                         val tail: Boolean,
                         val fileFilterMaybe: Option[String],
                         val watchersPair: Vector[(File, ActorRef)])
  extends Actor with ActorPublisher[SonicMessage] with SonicdLogging with LocalFilePublisher {

  override def parseUTF8Data(raw: String): Map[String, JsValue] =
    raw.parseJson.asJsObject.fields
}
