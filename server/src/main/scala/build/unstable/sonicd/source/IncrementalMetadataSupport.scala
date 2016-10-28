package build.unstable.sonicd.source

import akka.stream.actor.ActorPublisher
import build.unstable.sonic.model.{SonicMessage, TypeMetadata}
import build.unstable.sonicd.source.json.JsonUtils.JSONQuery
import spray.json.{JsObject, JsValue}

import scala.collection.mutable

trait IncrementalMetadataSupport {
  this: ActorPublisher[SonicMessage] ⇒

  val buffer: mutable.Queue[SonicMessage]
  val meta: TypeMetadata = null

  def onNext(query: JSONQuery, data: JsValue): Unit = {
    (data, query.select) match {
      case (JsObject(fields), Some(select)) ⇒
      case (JsObject(fields), None) ⇒
    }
    /* data match {
      // generate meta from fields and select
      case filtered if meta.isEmpty && query.select.isDefined ⇒
        val selected: Map[String, JsValue] = query.select.get
        val tmeta = TypeMetadata(selected)
        meta = Some(tmeta)
        buffer.enqueue(OutputChunk(JsArray(selected)))
        onNext(tmeta)
      // generate meta from fields
      case filtered if meta.isEmpty ⇒
        val selected = select(filtered)
        val tmeta = TypeMetadata(selected)
        meta = Some(tmeta)
        buffer.enqueue(OutputChunk(JsArray(select(meta, filtered))))
        onNext(tmeta)
      case filtered ⇒ onNext(tmeta)
    } */
  }
}
