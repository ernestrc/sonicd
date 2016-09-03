package build.unstable.sonic

import java.net.InetAddress
import java.util.concurrent.TimeUnit

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json._

import scala.concurrent.duration.FiniteDuration

trait JsonProtocol extends SprayJsonSupport with DefaultJsonProtocol {

  implicit val printer = CompactPrinter

  implicit object ClassJsonFormat extends RootJsonFormat[Class[_]] {
    def write(obj: Class[_]): JsValue = JsString(obj.getName)

    def read(json: JsValue): Class[_] = {
      json match {
        case JsString(s) ⇒ this.getClass.getClassLoader.loadClass(s)
        case anyElse ⇒
          throw new Exception("Class field should be a string representing the full class path of the class to load")
      }
    }
  }

  implicit val inetAddressJsonFormat: RootJsonFormat[InetAddress] = new RootJsonFormat[InetAddress] {
    override def read(json: JsValue): InetAddress = InetAddress.getByName(json.convertTo[String])

    override def write(obj: InetAddress): JsValue = JsString(obj.getHostName)
  }

  implicit val durationJsonFormat: RootJsonFormat[FiniteDuration] = new RootJsonFormat[FiniteDuration] {
    def read(json: JsValue): FiniteDuration = {
      val obj = json.asJsObject.fields
      FiniteDuration(obj("length").convertTo[Long], TimeUnit.valueOf(obj("unit").convertTo[String]))
    }

    def write(obj: FiniteDuration): JsValue = JsObject(Map(
      "length" → JsNumber(obj.length),
      "unit" → JsString(obj.unit.toString)
    ))
  }

}

object JsonProtocol extends JsonProtocol
