package build.unstable.sonicd.source

import build.unstable.sonic.model.{OutputChunk, SonicMessage, TypeMetadata}
import build.unstable.sonicd.source.json.JsonUtils.ParsedQuery

import scala.annotation.tailrec
import scala.collection.mutable

trait IncrementalMetadataSupport {

  import build.unstable.sonic.JsonProtocol._
  import spray.json._

  val buffer: mutable.Queue[SonicMessage]
  private var meta: TypeMetadata = TypeMetadata(Vector.empty)

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
