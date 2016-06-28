package build.unstable.sonicd.model

import java.nio.charset.Charset

import akka.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage}
import akka.util.ByteString
import build.unstable.sonicd.model.JsonProtocol._
import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import spray.json._

import scala.util.Try

sealed trait SonicMessage {

  val variation: Option[String]

  val payload: Option[JsValue]

  val eventType: String

  @transient
  lazy val json: JsValue = {
    val fields = scala.collection.mutable.ListBuffer(
      SonicMessage.eventType → (JsString(eventType): JsValue))

    variation.foreach(v ⇒ fields.append(SonicMessage.variation → JsString(v)))
    payload.foreach(p ⇒ fields.append(SonicMessage.payload → p))

    JsObject(fields.toMap)
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
  val eventType: String = SonicMessage.meta
}

case class OutputChunk(data: JsArray) extends SonicMessage {
  val variation: Option[String] = None
  val eventType = SonicMessage.out
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
  override val eventType = SonicMessage.progress
}

case class DoneWithQueryExecution(error: Option[Throwable] = None) extends SonicMessage {

  val success = error.isEmpty

  override val eventType = SonicMessage.done
  override val variation: Option[String] = error.map(e ⇒ getStackTrace(e))
  override val payload: Option[JsValue] = None

}

object DoneWithQueryExecution {
  val success: DoneWithQueryExecution = DoneWithQueryExecution(None)

  def error(e: Throwable): DoneWithQueryExecution = DoneWithQueryExecution(Some(e))
}

//events sent by the client to the server
case object ClientAcknowledge extends SonicMessage {
  override val variation: Option[String] = None
  override val payload: Option[JsValue] = None
  override val eventType: String = SonicMessage.ack
}

case class Log(message: String) extends SonicMessage {
  override val variation: Option[String] = Some(message)
  override val payload: Option[JsValue] = None
  override val eventType: String = SonicMessage.log
}

sealed trait SonicCommand extends SonicMessage {
  val traceId: Option[String]

  def setTraceId(trace_id: String): SonicCommand
}

case class Authenticate(user: String, key: String, traceId: Option[String])
  extends SonicCommand {
  override val variation: Option[String] = Some(key)
  override val payload: Option[JsValue] = Some(JsObject(Map(
    "user" → JsString(user),
    "trace_id" → traceId.map(JsString.apply).getOrElse(JsNull)
  )))
  override val eventType: String = SonicMessage.auth

  override def setTraceId(trace_id: String): SonicCommand =
    copy(traceId = Some(trace_id))

  override def toString: String = s"Authenticate($user)"
}

object SonicMessage {

  //fields
  val eventType = "e"
  val variation = "v"
  val payload = "p"

  //messages
  val auth = "H"
  val query = "Q"
  val meta = "T"
  val progress = "P"
  val log = "L"
  val out = "O"
  val ack = "A"
  val done = "D"

  def unapply(ev: SonicMessage): Option[(String, Option[String], Option[JsValue])] =
    Some((ev.eventType, ev.variation, ev.payload))

  def fromJson(raw: String): SonicMessage = try {
    val fields = raw.parseJson.asJsObject.fields

    val vari: Option[String] = fields.get(variation).flatMap(_.convertTo[Option[String]])
    val pay: Option[JsValue] = fields.get(payload)

    fields.get(eventType).map(_.convertTo[String]) match {
      case Some(`out`) ⇒ pay match {
        case Some(d: JsArray) ⇒ OutputChunk(d)
        case a ⇒ throw new Exception(s"expecting JsArray found $a")
      }
      case Some(`ack`) ⇒ ClientAcknowledge
      case Some(`auth`) ⇒
        val fields = pay.get.asJsObject.fields
        Authenticate(
          fields("user").convertTo[String],
          vari.get,
          fields.get("trace_id").flatMap(_.convertTo[Option[String]]))
      case Some(`log`) ⇒ Log(vari.get)
      case Some(`meta`) ⇒
        pay match {
          case Some(d: JsArray) ⇒ TypeMetadata(d.convertTo[Vector[(String, JsValue)]])
          case Some(j: JsObject) ⇒ TypeMetadata(j.convertTo[Vector[(String, JsValue)]])
          case a ⇒ throw new Exception(s"expecting JsArray found $a")
        }
      case Some(`progress`) ⇒
        QueryProgress(
          Try(pay.get.asJsObject.fields.get("progress").map(_.convertTo[Double])).toOption.flatten,
          Try(pay.get.asJsObject.fields.get("output").map(_.convertTo[String])).toOption.flatten
        )
      case Some(`query`) ⇒
        val p = pay.get.asJsObject.fields
        val traceId = p.get("trace_id").flatMap(_.convertTo[Option[String]])
        val token = p.get("auth").flatMap(_.convertTo[Option[String]])
        new Query(None, traceId, token, vari.get, p("config"))
      case Some(`done`) ⇒ DoneWithQueryExecution(vari.map(fromStackTrace))
      case Some(e) ⇒ throw new Exception(s"unexpected event type '$e'")
      case None ⇒ throw new Exception("no 'e' event_type")
    }
  } catch {
    case e: Exception ⇒ throw new Exception(s"sonic message deserialization error", e)
  }

  def fromBinary(m: BinaryMessage.Strict): SonicMessage =
    fromJson(m.data.utf8String)

  def fromBytes(b: ByteString): SonicMessage =
    fromJson(b.decodeString("utf-8"))
}

class Query(val id: Option[Long],
            val traceId: Option[String],
            val auth: Option[String],
            val query: String,
            val _config: JsValue)
  extends SonicCommand {

  override def setTraceId(trace_id: String): SonicCommand =
    copy(trace_id = Some(trace_id))

  override val variation: Option[String] = Some(query)
  override val payload: Option[JsValue] = {
    val fields = scala.collection.mutable.Map(
      "config" → _config
    )
    auth.foreach(j ⇒ fields.update("auth", JsString(j)))
    traceId.foreach(t ⇒ fields.update("trace_id", JsString(t)))
    Some(JsObject(fields.toMap))
  }
  override val eventType: String = SonicMessage.query

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

  override def toString: String = s"Query(id=$id,trace_id=$traceId)"

  private[sonicd] def getSourceClass: Class[_] = {
    val clazzLoader = this.getClass.getClassLoader
    Try(clazzLoader.loadClass(clazzName))
      .getOrElse(clazzLoader.loadClass("build.unstable.sonicd.source." + clazzName))
  }

  def copy(query_id: Option[Long] = None, trace_id: Option[String] = None) =
    new Query(query_id orElse id, trace_id orElse traceId, auth, query, _config)
}

object Query {

  /**
   * Build a Query from a fully specified source configuration 'config'
   */
  def apply(query: String, config: JsObject, authToken: Option[String]): Query =
    new Query(None, None, authToken, query, config)

  /**
   * Build a Query from a configuration alias 'config' for the sonicd server to
   * load from its configuration
   */
  def apply(query: String, config: JsString, authToken: Option[String]): Query =
    new Query(None, None, authToken, query, config)

  def unapply(query: Query): Option[(Option[Long], Option[String], String)] =
    Some((query.id, query.auth, query.query))
}
