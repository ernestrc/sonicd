package build.unstable.sonicd.system

import java.nio.charset.Charset
import java.util.UUID

import akka.actor._
import akka.actor.SupervisorStrategy.Restart
import akka.http.scaladsl.model.ws.{BinaryMessage, Message}
import akka.stream._
import akka.util.ByteString
import build.unstable.sonicd.model._

import scala.collection.mutable

class SonicController(materializer: Materializer) extends Actor with SonicdActorLogging {

  import SonicController._

  // logging turned off as children errors are not system errors
  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(loggingEnabled = true) {
    case e: Exception ⇒ Restart
  }

  val handlers = mutable.Map.empty[ActorPath, String]

  def receiptToBinaryMessage(rec: Receipt): Message =
    BinaryMessage.Strict(ByteString(
      JsonProtocol.receiptJsonFormat.write(rec).compactPrint.getBytes(Charset.defaultCharset())
    ))

  def newQuery(q: Query): (String, DataSource) = {
    val id = UUID.randomUUID().toString
    val query = q.copy(id)
    val source = query.getSourceClass.getConstructors()(0)
      .newInstance(query.config, query.query_id.get, query.query, context)
      .asInstanceOf[DataSource]
    (id, source)
  }

  override def receive: Receive = {

    //handler terminated
    case Terminated(ref) ⇒ handlers.get(ref.path) match {
      case Some(queryId) ⇒
        handlers.remove(ref.path)
      case None ⇒ log.warning(s"could not clean queryId of actor in ${ref.path}")
    }
      log.debug("handler terminated. living handlers: {}", handlers)

    case q@NewQuery(query) ⇒
      log.debug("client posted new query {}", q)
      val handler = sender()
      try {
        val (queryId, source) = newQuery(query)
        log.debug("successfully instantiated source {} for query with id '{}'", source, queryId)

        context watch handler
        handlers.update(handler.path, queryId)
        handler ! source.handlerProps

        log.debug(s"successfully instantiated protocol handler of query with id '$queryId'")
      } catch {
        case e: Exception ⇒
          val msg = "error when preparing stream materialization"
          log.error(e, msg)
          handler ! DoneWithQueryExecution.error(e)
      }
  }
}

object SonicController {

  case class NewQuery(query: Query) extends Command

  case class ExecuteQuery(queryId: String) extends Command

}
