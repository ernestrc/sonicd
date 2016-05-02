package build.unstable.sonicd.model

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json._

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

  implicit val receiptJsonFormat: RootJsonFormat[Receipt] = jsonFormat4(Receipt.apply)

  implicit val queryJsonFormat: RootJsonFormat[Query] = new RootJsonFormat[Query] {
    override def write(obj: Query): JsValue = JsObject(Map(
      "config" → obj.config,
      "query_id" → obj.query_id.map(JsString.apply).getOrElse(JsNull),
      "query" → JsString(obj.query)
    ))

    override def read(json: JsValue): Query = {
      val f = json.asJsObject.fields
      new Query(
        f.get("query_id").map(_.convertTo[String]),
        f("query").convertTo[String],
        f("config"))
    }
  }

}

object JsonProtocol extends JsonProtocol
