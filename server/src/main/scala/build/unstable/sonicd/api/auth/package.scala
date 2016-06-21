package build.unstable.sonicd.api

import build.unstable.sonicd.model.JsonProtocol._
import spray.json._

package object auth {

  implicit val modeJsonFormat: RootJsonFormat[Mode] = new RootJsonFormat[Mode] {
    override def write(obj: Mode): JsValue = JsString(obj match {
      case Mode.Read ⇒ "r"
      case Mode.ReadWrite ⇒ "rw"
    })

    override def read(json: JsValue): Mode = json.convertTo[String] match {
      case "r" | "read" ⇒ Mode.Read
      case "rw" | "write" ⇒ Mode.ReadWrite
    }
  }
  implicit val apiKeyJsonFormat: RootJsonFormat[ApiKey] = jsonFormat5(ApiKey.apply)
}
