package build.unstable.sonicd.api

import build.unstable.sonic.{JsonProtocol, ApiKey}
import JsonProtocol._
import spray.json._

package object auth {

  implicit val modeJsonFormat: RootJsonFormat[ApiKey.Mode] = new RootJsonFormat[ApiKey.Mode] {
    override def write(obj: ApiKey.Mode): JsValue = JsString(obj match {
      case ApiKey.Mode.Read ⇒ "r"
      case ApiKey.Mode.ReadWrite ⇒ "rw"
    })

    override def read(json: JsValue): ApiKey.Mode = json.convertTo[String] match {
      case "r" | "read" ⇒ ApiKey.Mode.Read
      case "rw" | "write" ⇒ ApiKey.Mode.ReadWrite
    }
  }
  implicit val apiKeyJsonFormat: RootJsonFormat[ApiKey] = jsonFormat5(ApiKey.apply)
}
