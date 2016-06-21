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

case class DoneWithQueryExecution(success: Boolean, errors: Vector[Throwable] = Vector.empty) extends SonicMessage {

  override val eventType = SonicMessage.done
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
  override val eventType: String = SonicMessage.ack
}

case class Log(message: String) extends SonicMessage {
  override val variation: Option[String] = Some(message)
  override val payload: Option[JsValue] = None
  override val eventType: String = SonicMessage.log
}

case class Authenticate(user: String, key: String) extends SonicMessage {
  override val variation: Option[String] = Some(user)
  override val payload: Option[JsValue] = Some(JsString(key))
  override val eventType: String = SonicMessage.auth
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

    val vari: Option[String] = Try(fields.get(variation).map(_.convertTo[String])).getOrElse(None)
    val pay: Option[JsValue] = fields.get(payload)

    fields.get(eventType).map(_.convertTo[String]) match {
      case Some(`out`) ⇒ pay match {
        case Some(d: JsArray) ⇒ OutputChunk(d)
        case a ⇒ throw new Exception(s"expecting JsArray found $a")
      }
      case Some(`ack`) ⇒ ClientAcknowledge
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
      case Some(`done`) ⇒ DoneWithQueryExecution(vari.get == "success",
        pay.map(_.convertTo[Vector[String]].map(e ⇒ new Exception(e))).getOrElse(Vector.empty))
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
            //added client
            val traceId: Option[String],
            val authToken: Option[String],
            val query: String,
            val _config: JsValue)
  extends SonicMessage {

  override val variation: Option[String] = Some(query)
  override val payload: Option[JsValue] = {
    val fields = scala.collection.mutable.Map(
      "config" → _config
    )
    authToken.foreach(j ⇒ fields.update("auth", JsString(j)))
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

  override def toString: String = s"Query($id,$query,$traceId)"

  private[sonicd] def getSourceClass: Class[_] = {
    val clazzLoader = this.getClass.getClassLoader
    Try(clazzLoader.loadClass(clazzName))
      .getOrElse(clazzLoader.loadClass("build.unstable.sonicd.source." + clazzName))
  }

  def copy(query_id: Option[Long] = None, trace_id: Option[String] = None) =
    new Query(query_id, trace_id, authToken, query, _config)
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
    Some((query.id, query.authToken, query.query))
}