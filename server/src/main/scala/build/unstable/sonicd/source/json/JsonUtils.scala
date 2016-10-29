package build.unstable.sonicd.source.json

import build.unstable.sonic.JsonProtocol._
import spray.json._

object JsonUtils {

  case class ParsedQuery(select: Option[Vector[String]], valueFilter: JsValue ⇒ Option[JsValue])

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
  def parseQuery(raw: String): ParsedQuery = {
    val r = raw.parseJson.asJsObject(s"Query must be a valid JSON object: $raw").fields
    parseQuery(r)
  }

  def parseQuery(r: Map[String, JsValue]): ParsedQuery = {
    val select = r.get("select").map { v ⇒
      v.convertTo[Vector[String]]
    }

    val valueFilter: JsValue ⇒ Option[JsValue] = r.get("filter").map {
      case JsObject(objFilter) ⇒
        val filter: PartialFunction[JsValue, Option[JsValue]] = {
          case j@JsObject(fields) ⇒ if (matchObject(objFilter)(fields)) Some(j) else None
          case _ ⇒ None
        }
        filter
      case j: JsString ⇒ (a: JsValue) ⇒ { if(a == j) Some(j) else None }
      case j: JsNumber ⇒ (a: JsValue) ⇒ { if(a == j) Some(j) else None }
      case JsNull ⇒ (a: JsValue) ⇒ { if (a == JsNull) Some(JsNull) else None }
      case j: JsBoolean ⇒ (a: JsValue) ⇒ { if (a == j) Some(j) else None }
      case j: JsArray ⇒ (a: JsValue) ⇒ { if (a == j) Some(j) else None }
    }.getOrElse((o: JsValue) ⇒ Some(o))

    ParsedQuery(select, valueFilter)
  }
}
