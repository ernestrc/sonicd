package build.unstable.sonic.model

import build.unstable.sonic.JsonProtocol
import spray.json._

import scala.util.{Failure, Success, Try}

private[unstable] trait AuthConfig {
  val provider: Class[_]
  val config: Map[String, JsValue]
}

private[unstable] object AuthConfig {

  import JsonProtocol._

  private val classLoader = this.getClass.getClassLoader

  implicit object AuthConfigFormat extends RootJsonFormat[AuthConfig] {
    override def write(obj: AuthConfig): JsValue = obj match {
      case SonicdAuth(t) ⇒ JsString(t)
      case _ ⇒
        JsObject(Map(
          "provider" → JsString(obj.provider.getName),
          "config" → JsObject(obj.config)
        ))
    }

    override def read(json: JsValue): AuthConfig = json match {
      case j: JsObject ⇒
        val fields = j.fields
        val provider = fields("provider").convertTo[String]
        val providerClass = classLoader.loadClass(provider)
        new AuthConfig {
          val provider: Class[_] = providerClass
          val config: Map[String, JsValue] = fields
        }
      // to mantain backwards compatibility
      case JsString(t) ⇒ SonicdAuth(t)
      case _ ⇒ throw new Exception(
        "`auth` expected to be either a string (token) or a configuration object with a `provider` and the fields required by that provider")
    }
  }

  /**
    * Read or ReadWrite access mode to Sonicd's sources
    */
  sealed trait Mode {
    def canWrite: Boolean
  }

  object Mode {

    def apply(str: String): Try[Mode] = str match {
      case "r" ⇒ Success(Read)
      case "rw" ⇒ Success(ReadWrite)
      case e ⇒ Failure(new Exception(s"unknown ApiKey.Mode '$e'"))
    }

    case object Read extends Mode {
      override def canWrite: Boolean = false

      override def toString: String = "r"
    }

    case object ReadWrite extends Mode {
      override def canWrite: Boolean = true

      override def toString: String = "rw"
    }

    implicit val jsonFormat: RootJsonFormat[AuthConfig.Mode] = new RootJsonFormat[AuthConfig.Mode] {
      override def read(json: JsValue): AuthConfig.Mode = json match {
        case JsString("rw") ⇒ AuthConfig.Mode.ReadWrite
        case JsString("r") ⇒ AuthConfig.Mode.Read
        case ss ⇒ throw new Exception(s"unexpected value for ApiKey.Mode: $ss")
      }

      override def write(obj: AuthConfig.Mode): JsValue = obj match {
        case AuthConfig.Mode.Read ⇒ JsString("r")
        case AuthConfig.Mode.ReadWrite ⇒ JsString("rw")
      }
    }
  }

}

case class SonicdAuth(token: String) extends AuthConfig {
  override val provider: Class[_] = classOf[Nothing]
  override val config: Map[String, JsValue] = Map.empty
}
