package build.unstable.sonicd.source

import akka.actor.ActorContext
import build.unstable.sonic.model.{DataSource, Query, RequestContext}
import build.unstable.sonicd.source.SonicdSource._
import build.unstable.sonicd.system.actor.SonicdController._
import spray.json.JsonFormat

abstract class SonicdSource(query: Query, actorContext: ActorContext, context: RequestContext)
  extends DataSource(query, actorContext, context) {

  def getConfig[T: JsonFormat](key: String): T =
    query.sonicdConfig.fields.get(key).map(_.convertTo[T])
      .getOrElse(throw new MissingConfigurationException(key))

  def getOption[T: JsonFormat](key: String): Option[T] =
    query.sonicdConfig.fields.get(key).map(_.convertTo[T])

}

object SonicdSource {

  class MissingConfigurationException(missing: String) extends Exception(s"config is missing '$missing' field")

}
