package build.unstable.sonicd

import java.nio.charset.Charset

import akka.http.scaladsl.model.ws.{TextMessage, BinaryMessage, Message}
import akka.util.ByteString
import build.unstable.sonicd.model.JsonProtocol._
import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
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
      TextMessage.Strict(json.compactPrint)

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

  case class QueryProgress(progress: Option[Double], output: Option[String]) extends SonicMessage {
    val payload: Option[JsValue] = Some(JsObject(Map(
      "progress" → progress.map(JsNumber.apply).getOrElse(JsNull),
      "output" → output.map(JsString.apply).getOrElse(JsNull)
    )))
    val variation = None
    override val eventType = Some("P")
  }

  case class DoneWithQueryExecution(success: Boolean, errors: Vector[Throwable] = Vector.empty) extends SonicMessage {

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
    val success: DoneWithQueryExecution = DoneWithQueryExecution(success = true, Vector.empty)
    def error(e: Throwable): DoneWithQueryExecution =
      DoneWithQueryExecution(success = false, Vector(e))
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
              Try(payload.get.asJsObject.fields.get("progress").map(_.convertTo[Double])).toOption.flatten,
              Try(payload.get.asJsObject.fields.get("output").map(_.convertTo[String])).toOption.flatten
            )
          case Some("Q") ⇒ new Query(None, variation.get, payload.get)
          case Some("D") ⇒ DoneWithQueryExecution(variation.get == "success",
            payload.map(_.convertTo[Vector[String]].map(e ⇒ new Exception(e))).getOrElse(Vector.empty))
          case e ⇒ throw new Exception(s"unexpected event type '$e'")
        }
      } else {
        val queryStr = fields.getOrElse("query",
          throw new Exception("missing 'query' field in query payload")).convertTo[String]
        val config: JsValue = fields.getOrElse("config",
          throw new Exception("missing 'config' field in query payload"))
        new Query(None, queryStr, config)
      }
    } catch {
      case e: Exception ⇒ throw new Exception(s"event deserialization error: ${e.getMessage}", e)
    }

    def fromBinary(m: BinaryMessage.Strict): SonicMessage =
      fromJson(m.data.utf8String)

    def fromBytes(b: ByteString): SonicMessage =
      fromJson(b.decodeString("utf-8"))
  }

  class Query(val query_id: Option[String], val query: String, val _config: JsValue) extends SonicMessage {

    override val variation: Option[String] = Some(query)
    override val payload: Option[JsValue] = Some(_config)
    override val eventType: Option[String] = Some("Q")

    //CAUTION: leaking this value outside of sonicd-server is a major security risk
    private[sonicd] lazy val config = _config match {
      case o: JsObject ⇒ o
      case JsString(alias) ⇒ Try {
        ConfigFactory.load().getObject(s"sonicd.source.$alias")
          .render(ConfigRenderOptions.concise()).parseJson.asJsObject
      }.recover {
        case e: Exception ⇒ throw new Exception(s"could not load query config '$alias'", e)
      }.get
      case _ ⇒
        throw new Exception("'config' key in query config can only be either a full config " +
          "object or an alias (string) that will be extracted by sonicd server")
    }

    private[sonicd] def clazzName: String = config.fields.getOrElse("class",
      throw new Exception(s"missing key 'class' in config")).convertTo[String]

    private[sonicd] def getSourceClass: Class[_] = {
      val clazzLoader = this.getClass.getClassLoader
      Try(clazzLoader.loadClass(clazzName))
        .getOrElse(clazzLoader.loadClass("build.unstable.sonicd.source." + clazzName))
    }

    def copy(query_id: String) = new Query(Some(query_id), query, _config)
  }

  object Query {

    /**
     * Build a Query from a fully specified source configuration 'config'
     */
    def apply(query: String, config: JsObject): Query = new Query(None, query, config)

    /**
     * Build a Query from a configuration alias 'config' for the sonicd server to
     * load from its configuration
     */
    def apply(query: String, config: JsString): Query = new Query(None, query, config)

    def unapply(query: Query): Option[(Option[String], String)] = Some((query.query_id, query.query))
  }

}
