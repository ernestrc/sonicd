package build.unstable.sonicd.model

import akka.actor._
import build.unstable.sonicd.auth.ApiUser
import build.unstable.sonicd.model.DataSource.ConfigurationException
import build.unstable.sonicd.model.JsonProtocol._
import spray.json._

object DataSource {

  class ConfigurationException(missing: String) extends Exception(s"config is missing '$missing' field")

}

abstract class DataSource(config: JsObject, queryId: String, query: String,
                          context: ActorContext, user: Option[ApiUser]) {

  def securityLevel: Option[Int] = config.fields.get("security").map(_.convertTo[Int])

  def getConfig[T: JsonFormat](key: String): T =
    config.fields.get(key).map(_.convertTo[T]).getOrElse(throw new ConfigurationException(key))

  def getOption[T: JsonFormat](key: String): Option[T] = config.fields.get(key).map(_.convertTo[T])

  def handlerProps: Props

}
