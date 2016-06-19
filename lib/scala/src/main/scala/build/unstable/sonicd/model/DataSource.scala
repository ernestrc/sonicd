package build.unstable.sonicd.model

import akka.actor._
import build.unstable.sonicd.model.DataSource.{LongLong, ConfigurationException}
import spray.json._
import JsonProtocol._

import scala.runtime.RichLong

object DataSource {

  class ConfigurationException(missing: String) extends Exception(s"config is missing '$missing' field")

  //to get around "the result type of an implicit conversion must be more specific than AnyRef"
  case class LongLong(long: Long)

}

abstract class DataSource(config: JsObject, queryId: String, query: String, context: ActorContext) {

  def securityLevel: Option[Int] = config.fields.get("security").map(_.convertTo[Int])

  def getConfig[T: JsonFormat](key: String): T =
    config.fields.get(key).map(_.convertTo[T]).getOrElse(throw new ConfigurationException(key))

  def getOption[T: JsonFormat](key: String): Option[T] = config.fields.get(key).map(_.convertTo[T])

  def handlerProps: Props

}
