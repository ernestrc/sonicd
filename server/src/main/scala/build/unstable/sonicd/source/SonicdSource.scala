package build.unstable.sonicd.source

import akka.actor.ActorContext
import build.unstable.sonic.model.{DataSource, Query, RequestContext}
import build.unstable.sonicd.SonicdLogging
import build.unstable.sonicd.source.SonicdSource._
import build.unstable.sonicd.system.actor.SonicdController._
import spray.json.JsonFormat
import build.unstable.sonic.JsonProtocol._

abstract class SonicdSource(query: Query, actorContext: ActorContext, context: RequestContext)
  extends DataSource(query, actorContext, context) with SonicdLogging {

  def getConfig[T: JsonFormat](key: String): T = {
    val value = query.sonicdConfig.fields.get(key).flatMap(_.convertTo[Option[T]])
      .getOrElse(throw new MissingConfigurationException(key))
    log.debug("getConfig({})={}", key, value)
    value
  }

  def getOption[T: JsonFormat](key: String): Option[T] = {
    val value = query.sonicdConfig.fields.get(key).flatMap(_.convertTo[Option[T]])
    log.debug("getOption({})={}", key, value)
    value
  }

}

object SonicdSource {

  class MissingConfigurationException(missing: String) extends Exception(s"config is missing '$missing' field")

}
