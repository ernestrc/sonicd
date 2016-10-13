package build.unstable.sonicd.auth

import build.unstable.sonic.AuthConfig
import build.unstable.sonic.JsonProtocol._
import spray.json._

package object auth {

  implicit val modeJsonFormat: RootJsonFormat[AuthConfig.Mode] = new RootJsonFormat[AuthConfig.Mode] {
    override def write(obj: AuthConfig.Mode): JsValue = JsString(obj match {
      case AuthConfig.Mode.Read ⇒ "r"
      case AuthConfig.Mode.ReadWrite ⇒ "rw"
    })

    override def read(json: JsValue): AuthConfig.Mode = json.convertTo[String] match {
      case "r" | "read" ⇒ AuthConfig.Mode.Read
      case "rw" | "write" ⇒ AuthConfig.Mode.ReadWrite
    }
  }
  implicit val apiKeyJsonFormat: RootJsonFormat[ApiKey] = jsonFormat5(ApiKey.apply)
}
