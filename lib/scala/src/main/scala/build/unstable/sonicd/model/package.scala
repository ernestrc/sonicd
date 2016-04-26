package build.unstable.sonicd

import java.nio.charset.Charset

import akka.http.scaladsl.model.ws.{BinaryMessage, Message}
import akka.util.ByteString
import build.unstable.sonicd.model.JsonProtocol._
import org.slf4j.LoggerFactory
import spray.json._

import scala.util.Try

package object model {

  sealed trait SonicMessage {

    val variation: Option[String]

    val payload: Option[JsValue]

    val eventType: Option[String]

    @transient
    lazy val json: JsValue = {
      JsObject(Map(
        ("event_type", eventType.map(JsString.apply).getOrElse(JsNull)),
        ("variation", variation.map(JsString.apply).getOrElse(JsNull)),
        ("payload", payload.getOrElse(JsNull))
      ))
    }

    def toBytes: ByteString = {
      val bytes = json.compactPrint.getBytes(Charset.forName("utf-8"))
      ByteString(bytes)
    }

    def toWsMessage: Message =
      BinaryMessage.Strict(ByteString(json.compactPrint.getBytes(Charset.forName("utf-8"))))

    def isDone: Boolean = this.isInstanceOf[DoneWithQueryExecution]
  }

  //events sent by the server to the client
  case class TypeMetadata(typesHint: Vector[(String, JsValue)]) extends SonicMessage {
    val variation: Option[String] = None
    val payload: Option[JsValue] = Some(typesHint.toJson)
    val eventType: Option[String] = Some("T")
  }

  case class OutputChunk(data: JsArray) extends SonicMessage {
    val variation: Option[String] = None
    val eventType = Some("O")
    val payload: Option[JsValue] = Some(data.toJson)
  }

  object OutputChunk {
    def apply[T: JsonWriter](data: Vector[T]): OutputChunk = OutputChunk(JsArray(data.map(_.toJson)))
  }

  case class QueryProgress(progress: Option[Int], output: Option[String]) extends SonicMessage {
    val payload: Option[JsValue] = Some(JsObject(Map(
      "progress" → progress.map(JsNumber.apply).getOrElse(JsNull),
      "output" → output.map(JsString.apply).getOrElse(JsNull)
    )))
    val variation = None
    override val eventType = Some("P")
  }

  case class DoneWithQueryExecution(success: Boolean, errors: Vector[Exception] = Vector.empty) extends SonicMessage {

    override val eventType = Some("D")
    override val variation: Option[String] = if (success) Some("success") else Some("error")

    override val payload: Option[JsValue] = {
      Some(JsArray(errors.map { e ⇒
        val cause = e.getCause
        val rr = Receipt.getStackTrace(e)
        JsString(if (cause != null) rr + "\ncause: " + Receipt.getStackTrace(e.getCause) else rr)
      }))
    }

  }

  object DoneWithQueryExecution {
    def error(e: Exception): DoneWithQueryExecution =
      DoneWithQueryExecution(success = false, Vector(e))

    def error(e: Throwable): DoneWithQueryExecution = {
      error(new Exception(e.getMessage, e.getCause))
    }
  }

  //events sent by the client to the server
  case object ClientAcknowledge extends SonicMessage {
    override val variation: Option[String] = None
    override val payload: Option[JsValue] = None
    override val eventType: Option[String] = Some("A")
  }

  object SonicMessage {

    def unapply(ev: SonicMessage): Option[(Option[String], Option[String], Option[JsValue])] =
      Some(ev.eventType, ev.variation, ev.payload)

    def fromJson(raw: String): SonicMessage = try {
      val fields = raw.parseJson.asJsObject.fields
      if (fields.contains("event_type")) {
        val variation: Option[String] = Try(fields.get("variation").map(_.convertTo[String])).getOrElse(None)
        val payload: Option[JsValue] = fields.get("payload")
        fields.get("event_type").map(_.convertTo[String]) match {
          case Some("O") ⇒ payload match {
            case Some(d: JsArray) ⇒ OutputChunk(d)
            case a ⇒ throw new Exception(s"expecting JsArray found $a")
          }
          case Some("A") ⇒ ClientAcknowledge
          case Some("T") ⇒
            payload match {
              case Some(d: JsArray) ⇒ TypeMetadata(d.convertTo[Vector[(String, JsValue)]])
              case Some(j: JsObject) ⇒ TypeMetadata(j.convertTo[Vector[(String, JsValue)]])
              case a ⇒ throw new Exception(s"expecting JsArray found $a")
            }

          case Some("P") ⇒
            QueryProgress(
              Try(payload.get.asJsObject.fields.get("progress").map(_.convertTo[Int])).toOption.flatten,
              Try(payload.get.asJsObject.fields.get("output").map(_.convertTo[String])).toOption.flatten
            )
          case Some("Q") ⇒ Query(None, variation.get, payload.get.asJsObject)
          case Some("D") ⇒ DoneWithQueryExecution(variation.get == "success",
            payload.map(_.convertTo[Vector[String]].map(e ⇒ new Exception(e))).getOrElse(Vector.empty))
          case e ⇒ throw new Exception(s"unexpected event type '$e'")
        }
      } else {
        val queryStr = fields.getOrElse("query",
          throw new Exception("missing 'query' field in query payload")).convertTo[String]
        val config = fields.getOrElse("config",
          throw new Exception("missing 'config' field in query payload")).asJsObject
        Query(queryStr, config)
      }
    } catch {
      case e: Exception ⇒ throw new Exception(s"event deserialization error: ${e.getMessage}", e)
    }

    def fromBinary(m: BinaryMessage.Strict): SonicMessage =
      fromJson(m.data.utf8String)

    def fromBytes(b: ByteString): SonicMessage =
      fromJson(b.decodeString("utf-8"))
  }

  case class Query(query_id: Option[String], query: String, config: JsObject) extends SonicMessage {

    @transient
    override val variation: Option[String] = Some(query)
    @transient
    override val payload: Option[JsValue] = Some(config)
    @transient
    override val eventType: Option[String] = Some("Q")

    @transient
    lazy val clazzName = config.fields.getOrElse("class",
      throw new Exception(s"missing key 'class' in config")).convertTo[String]

    def getSourceClass: Class[_] = Try(Query.clazzLoader.loadClass(clazzName))
      .getOrElse(Query.clazzLoader.loadClass("build.unstable.sonicd.source." + clazzName))
  }

  object Query {

    def apply(query: String, config: JsObject): Query = new Query(None, query, config)

    import spray.json._

    val clazzLoader = this.getClass.getClassLoader

    def fromBytes(data: ByteString): Try[Query] = Try {
      val msg = SonicMessage.fromBytes(data)
      Query(None, msg.variation.getOrElse(throw new Exception("protocol error: query msg is missing 'variation' field")),
        msg.payload.map(_.asJsObject).getOrElse(JsObject.empty))
    }
  }

}
