package build.unstable.sonicd.source

import build.unstable.sonic.JsonProtocol._
import build.unstable.sonic.model.{OutputChunk, SonicMessage, TypeMetadata}
import spray.json._

import scala.annotation.tailrec
import scala.collection.mutable

object SonicdPublisher {

  case class ParsedQuery(select: Option[Vector[String]], valueFilter: JsValue ⇒ Option[JsValue])

}

trait SonicdPublisher {

  import build.unstable.sonicd.source.SonicdPublisher._

  val buffer: mutable.Queue[SonicMessage]

  private var meta: TypeMetadata = TypeMetadata(Vector.empty)

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
      case j: JsString ⇒ (a: JsValue) ⇒ {
        if (a == j) Some(j) else None
      }
      case j: JsNumber ⇒ (a: JsValue) ⇒ {
        if (a == j) Some(j) else None
      }
      case JsNull ⇒ (a: JsValue) ⇒ {
        if (a == JsNull) Some(JsNull) else None
      }
      case j: JsBoolean ⇒ (a: JsValue) ⇒ {
        if (a == j) Some(j) else None
      }
      case j: JsArray ⇒ (a: JsValue) ⇒ {
        if (a == j) Some(j) else None
      }
    }.getOrElse((o: JsValue) ⇒ Some(o))

    ParsedQuery(select, valueFilter)
  }

  def select(data: Map[String, JsValue], selection: Vector[String]): Map[String, JsValue] =
    selection.map(s ⇒ data.get(s) match {
      case Some(v) ⇒ (s, v)
      case None ⇒ (s, JsNull)
    }).toMap

  def getEmpty(value: JsValue): JsValue = value match {
    case j: JsString ⇒ JsString.empty
    case j: JsNumber ⇒ JsNumber.zero
    case j: JsBoolean ⇒ JsBoolean(true)
    case JsNull ⇒ JsNull
    case j: JsArray ⇒ JsArray.empty
    case j: JsObject ⇒ JsObject.empty
  }

  // combines two types metadata by appending new keys if not existent or updating type if they do exist
  @tailrec
  final def mergeMeta(prev: Vector[(String, JsValue)], current: Vector[(String, JsValue)]): Vector[(String, JsValue)] =
  current.headOption match {
    case Some((key, value)) ⇒
      val idx = prev.indexWhere(_._1 == key)
      if (idx >= 0) mergeMeta(prev.updated(idx, (key, value)), current.tail)
      else mergeMeta(prev.:+((key, value)), current.tail)
    case None ⇒ prev
  }

  // returns true if meta was updated
  def updateMeta(m: TypeMetadata): Boolean = meta.typesHint != m.typesHint && {
    val old = meta
    meta = TypeMetadata(mergeMeta(meta.typesHint, m.typesHint))
    meta != old
  }

  def extractMeta(data: Map[String, JsValue]): TypeMetadata =
    TypeMetadata(data.foldLeft(Vector.empty[(String, JsValue)]) { (acc, kv) ⇒
      acc.:+((kv._1, getEmpty(kv._2)))
    })

  def alignOutput(data: Map[String, JsValue], meta: TypeMetadata): OutputChunk =
    OutputChunk(meta.typesHint.foldLeft(Vector.empty[JsValue]) { (acc, d) ⇒
      acc.:+(data.getOrElse(d._1, JsNull))
    })

  def bufferNext(query: ParsedQuery, data: JsValue): Unit = {
    (query.valueFilter(data), query.select) match {

      case (Some(JsObject(fields)), Some(selection)) ⇒
        val selected = select(fields, selection)
        val extracted = extractMeta(selected)
        if (updateMeta(extracted)) buffer.enqueue(meta)
        val aligned = alignOutput(selected, meta)
        buffer.enqueue(aligned)

      case (Some(JsObject(fields)), None) ⇒
        val extracted = extractMeta(fields)
        if (updateMeta(extracted)) buffer.enqueue(meta)
        val aligned = alignOutput(fields, meta)
        buffer.enqueue(aligned)

      case (Some(jsValue), Some(selection)) ⇒
        val fields = Map("raw" → jsValue)
        val selected = select(fields, selection)
        val extracted = extractMeta(selected)
        if (updateMeta(extracted)) buffer.enqueue(meta)

      case (Some(jsValue), None) ⇒
        val fields = Map("raw" → jsValue)
        val extracted = extractMeta(fields)
        if (updateMeta(extracted)) buffer.enqueue(meta)
        val aligned = alignOutput(fields, meta)
        buffer.enqueue(aligned)
      case (None, _) ⇒ //filtered out
    }
  }
}
