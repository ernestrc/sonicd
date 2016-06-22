package build.unstable.sonicd.model

import akka.actor._
import build.unstable.sonicd.model.DataSource.ConfigurationException
import build.unstable.sonicd.model.JsonProtocol._
import spray.json._

object DataSource {

  class ConfigurationException(missing: String) extends Exception(s"config is missing '$missing' field")

}

abstract class DataSource(query: Query, actorContext: ActorContext, context: RequestContext) {

  def securityLevel: Option[Int] = query.config.fields.get("security").map(_.convertTo[Int])

  def getConfig[T: JsonFormat](key: String): T =
    query.config.fields.get(key).map(_.convertTo[T]).getOrElse(throw new ConfigurationException(key))

  def getOption[T: JsonFormat](key: String): Option[T] = query.config.fields.get(key).map(_.convertTo[T])

  def handlerProps: Props

}
