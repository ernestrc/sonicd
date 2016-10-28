package build.unstable.sonicd.source

import akka.actor.ActorContext
import build.unstable.sonic.model.{DataSource, Query, RequestContext}
import build.unstable.sonicd.source.SonicdSource._
import spray.json.JsonFormat

abstract class SonicdSource(query: Query, actorContext: ActorContext, context: RequestContext)
  extends DataSource(query, actorContext, context) {

  val sonicdConfig = query.config.asJsObject("query config has not been resolved: this is a bug in Sonicd")

  def getConfig[T: JsonFormat](key: String): T =
    sonicdConfig.fields.get(key).map(_.convertTo[T]).getOrElse(throw new ConfigurationException(key))

  def getOption[T: JsonFormat](key: String): Option[T] = sonicdConfig.fields.get(key).map(_.convertTo[T])

}

object SonicdSource {

  class ConfigurationException(missing: String) extends Exception(s"config is missing '$missing' field")

}
