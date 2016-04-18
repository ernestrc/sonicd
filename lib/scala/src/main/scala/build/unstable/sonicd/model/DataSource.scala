package build.unstable.sonicd.model

import akka.actor._
import akka.stream.scaladsl.Flow
import spray.json.JsObject

abstract class DataSource(config: JsObject, queryId: String, query: String, context: ActorContext) {

  def handlerProps: Props

}
