package build.unstable.sonicd.system.actor

import java.nio.charset.Charset

import akka.actor.SupervisorStrategy.Restart
import akka.actor._
import akka.http.scaladsl.model.ws.{BinaryMessage, Message}
import akka.stream._
import akka.util.ByteString
import build.unstable.sonicd.api.auth.ApiUser
import build.unstable.sonicd.model._
import build.unstable.sonicd.system.actor.SonicController.{TokenValidationResult, UnauthorizedException}
import build.unstable.tylog.Variation

import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

class SonicController(authService: ActorRef) extends Actor with SonicdLogging {

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    info(log, "starting Sonic Controller {}", self.path)
  }

  @throws[Exception](classOf[Exception])
  override def postRestart(reason: Throwable): Unit = {
    error(log, reason, "RESTARTED CONTROLLER")
  }

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(loggingEnabled = true) {
    case NonFatal(e) ⇒ Restart
  }


  /* HELPERS */

  def receiptToBinaryMessage(rec: Receipt): Message =
    BinaryMessage.Strict(ByteString(
      JsonProtocol.receiptJsonFormat.write(rec).compactPrint.getBytes(Charset.defaultCharset())
    ))

  def prepareMaterialization(handler: ActorRef, q: Query, user: Option[ApiUser]): Unit = {
    try {
      handled += 1
      val queryId = handled
      val query = q.copy(query_id = Some(queryId))
      val source = query.getSourceClass.getConstructors()(0)
        .newInstance(query.config, query.id.get.toString, query.query, context)
        .asInstanceOf[DataSource]

      debug(log, "successfully instantiated source {} for query with id '{}'", source, queryId)

      if (isAuthorized(user, source.securityLevel)) {
        context watch handler
        handlers.update(queryId, handler.path)
        handler ! source.handlerProps
      } else handler ! DoneWithQueryExecution.error(new UnauthorizedException(user))
    } catch {
      case e: Exception ⇒
        error(log, e, "error when preparing stream materialization")
        handler ! DoneWithQueryExecution.error(e)
    }
  }

  def isAuthorized(user: Option[ApiUser], security: Option[Int]): Boolean = {
    (user, security) match {
      case (_, None) ⇒ true
      case (Some(u), Some(s)) if u.autorization >= s ⇒ true
      case _ ⇒ false
    }
  }


  /* STATE */

  val handlers = mutable.Map.empty[Long, ActorPath]
  var handled: Long = 0


  /* BEHAVIOUR */

  override def receive: Receive = {

    //handler terminated
    case Terminated(ref) ⇒ handlers.find(_._2 == ref.path) match {
      case Some((id, path)) ⇒
        handlers.remove(id)
      case None ⇒ warning(log, "could not clean queryId of actor in {}", ref.path)
    }
      log.debug("handler terminated. living handlers: {}", handlers)

    case TokenValidationResult(result, query, handler) ⇒
      result match {
        case Success(user) ⇒ prepareMaterialization(handler, query, Some(user))
          trace(log, query.traceId.get, GetTokenValidation, Variation.Success, "validated token")
        case Failure(e) ⇒ handler ! DoneWithQueryExecution.error(e)
          trace(log, query.traceId.get, GetTokenValidation, Variation.Failure(e), "token validation failed")
      }

    case query: Query ⇒
      debug(log, "client posted new query {}", query)
      val handler = sender()

      query.authToken match {
        case Some(token) ⇒
          trace(log, query.traceId.get, GetTokenValidation,
            Variation.Attempt, "sending token {} for validation", token)
          authService ! AuthenticationActor.ValidateTokenForQuery(token, query, handler)
        case None ⇒ prepareMaterialization(handler, query, None)
      }

    case m ⇒ warning(log, "oops! It looks like I received the wrong message: {}", m)

  }
}

object SonicController {

  class UnauthorizedException(user: Option[ApiUser])
    extends Exception(user.map(u ⇒ s"user ${u.user} is unauthorized to access this source")
      .getOrElse("unauthenticated user cannot access this source"))

  case object GetHandlers

  case class TokenValidationResult(user: Try[ApiUser], query: Query, handler: ActorRef)

}
