package build.unstable.sonicd.model

import akka.actor._
import build.unstable.sonicd.model.DataSource.ConfigurationException
import spray.json.{JsObject, JsonFormat}

object DataSource {

  class ConfigurationException(missing: String) extends Exception(s"config is missing '$missing' field")

}

abstract class DataSource(config: JsObject, queryId: String, query: String, context: ActorContext) {

  def getConfig[T: JsonFormat](key: String): T =
    config.fields.get(key).map(_.convertTo[T]).getOrElse(throw new ConfigurationException(key))

  def getOption[T: JsonFormat](key: String): Option[T] = config.fields.get(key).map(_.convertTo[T])

  def handlerProps: Props

}
