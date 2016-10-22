package build.unstable.sonicd.source.json

import build.unstable.sonic.JsonProtocol._
import build.unstable.sonic.model.TypeMetadata
import spray.json._

object JsonUtils {

  case class JSONQuery(select: Option[Vector[String]],
                       valueFilter: Map[String, JsValue] ⇒ Boolean)

  def matchObject(filter: Map[String, JsValue]): Map[String, JsValue] ⇒ Boolean = (data: Map[String, JsValue]) ⇒ {
    filter.forall {
      case (key, j: JsObject) if data.isDefinedAt(key) && data(key).isInstanceOf[JsObject] ⇒
        matchObject(j.fields)(data(key).asInstanceOf[JsObject].fields)

      case (key, JsString(s)) if data.isDefinedAt(key) && data(key).isInstanceOf[JsString] && s.startsWith("*") ⇒
        data(key).convertTo[String].endsWith(s.tail)

      case (key, JsString(s)) if data.isDefinedAt(key) && data(key).isInstanceOf[JsString] && s.endsWith("*") ⇒
        data(key).convertTo[String].endsWith(s.tail)

      case (key, value) ⇒ data.isDefinedAt(key) && data(key) == value
    }

    filter.forall { case ((key, matchValue)) ⇒
      lazy val value = data(key)
      lazy val valueIsString = value.isInstanceOf[JsString]
      lazy val matchIsString = matchValue.isInstanceOf[JsString]
      lazy val valueString = value.convertTo[String]
      lazy val matchString = matchValue.convertTo[String]
      // data is not defined and match filter is null
      (!data.isDefinedAt(key) && matchValue == JsNull) || (data.isDefinedAt(key) && (
        // * at the beginning of the query so value endsWith
        (matchIsString && valueIsString && matchString.startsWith("*") && valueString.endsWith(matchString.substring(1, matchString.length))) ||
          // * at the end of the query so value startsWith
          (matchIsString && valueIsString && matchString.endsWith("*") && valueString.startsWith(matchString.substring(0, matchString.length - 1))) ||
          // equality match
          (value == matchValue)
        ))
    }
  }

  /**
    * {
    * "select" : ["field1", "field2"],
    * "filter" : { "field1" : "value1" }
    * }
    */
  def parseQuery(raw: String): JSONQuery = {
    val r = raw.parseJson.asJsObject(s"Query must be a valid JSON object: $raw").fields
    parseQuery(r)
  }

  def parseQuery(r: Map[String, JsValue]): JSONQuery = {
    val select = r.get("select").map { v ⇒
      v.convertTo[Vector[String]]
    }

    val valueFilter = r.get("filter").map { fo ⇒
      val fObj = fo.asJsObject(s"filter key must be a valid JSON object: ${fo.compactPrint}").fields
      matchObject(fObj)
    }.getOrElse((o: Map[String, JsValue]) ⇒ true)

    JSONQuery(select, valueFilter)
  }

  def select(meta: Option[TypeMetadata], fields: Map[String, JsValue]): Vector[JsValue] = {
    meta match {
      case Some(m) ⇒ m.typesHint.map {
        case (s: String, v: JsValue) ⇒ fields.getOrElse(s, JsNull)
      }
      case None ⇒ fields.values.to[Vector]
    }
  }

  def filter(data: Map[String, JsValue], query: JSONQuery, target: String): Option[Map[String, JsValue]] = {
    if (query.valueFilter(data) && target != "application.conf" && target != "reference.conf") {
      if (query.select.isEmpty) Some(data)
      else {
        val fields = data.filter(kv ⇒ query.select.get.contains(kv._1))
        Some(fields)
      }
    } else None
  }
}
