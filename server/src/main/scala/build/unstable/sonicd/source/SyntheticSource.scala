package build.unstable.sonicd.source

import akka.actor.{ActorContext, Props}
import build.unstable.sonic.model.{Query, RequestContext}
import build.unstable.sonic.server.source.SyntheticPublisher
import spray.json.JsObject
import build.unstable.sonic.JsonProtocol._

class SyntheticSource(query: Query, actorContext: ActorContext, context: RequestContext)
  extends SonicdSource(query, actorContext, context) {

  val publisher: Props = {
    val seed = getOption[Int]("seed")
    val size = getOption[Int]("size")
    val progress = getOption[Int]("progress-delay").getOrElse(10)
    val indexed = getOption[Boolean]("indexed").getOrElse(false)

    //user pre-defined schema
    val schema = getOption[JsObject]("schema")

    Props(classOf[SyntheticPublisher], seed, size,
      progress, query.query, indexed, schema, context)
  }
}
