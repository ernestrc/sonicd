package build.unstable.sonicd.system.actor

import java.net.InetAddress
import java.nio.charset.Charset

import akka.actor.SupervisorStrategy.Restart
import akka.actor._
import akka.http.scaladsl.model.ws.{BinaryMessage, Message}
import akka.pattern._
import akka.util.{ByteString, Timeout}
import build.unstable.sonicd.auth.ApiUser
import build.unstable.sonicd.model._
import build.unstable.sonicd.system.actor.SonicController.{NewQuery, UnauthorizedException}
import build.unstable.tylog.Variation

import scala.collection.mutable
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

class SonicController(authService: ActorRef, authenticationTimeout: Timeout) extends Actor with SonicdLogging {

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

  import context.dispatcher


  /* HELPERS */

  def prepareMaterialization(handler: ActorRef, q: Query,
                             user: Option[ApiUser], clientAddress: Option[InetAddress]): Unit = {
    try {
      handled += 1
      val queryId = handled
      val query = q.copy(query_id = Some(queryId))
      val source = query.getSourceClass.getConstructors()(0)
        .newInstance(query, context, RequestContext(query.traceId.get, user)).asInstanceOf[DataSource]

      debug(log, "successfully instantiated source {} for query with id '{}'", source, queryId)

      if (isAuthorized(user, source.securityLevel, clientAddress)) {
        context watch handler
        handlers.update(queryId, handler.path)
        handler ! source.handlerProps
      } else handler ! DoneWithQueryExecution.error(new UnauthorizedException(user, clientAddress))
    } catch {
      case e: Exception ⇒
        error(log, e, "error when preparing stream materialization")
        handler ! DoneWithQueryExecution.error(e)
    }
  }

  def isAuthorized(user: Option[ApiUser], security: Option[Int], clientAddress: Option[InetAddress]): Boolean = {
    (user, security, clientAddress) match {
      case (None, None, _) ⇒ true
      case (Some(u), None, Some(a)) if u.allowedIps.isEmpty || u.allowedIps.get.contains(a) ⇒ true
      case (Some(u), Some(s), a) if u.authorization >= s
        && (u.allowedIps.isEmpty || (a.isDefined && u.allowedIps.get.contains(a.get))) ⇒ true
      case _ ⇒ false
    }
  }


  /* STATE */

  val handlers = mutable.Map.empty[Long, ActorPath]
  var handled: Long = 0

  case class TokenValidationResult(user: Try[ApiUser], query: Query,
                                   handler: ActorRef, clientAddress: Option[InetAddress])

  /* BEHAVIOUR */

  override def receive: Receive = {

    //handler terminated
    case Terminated(ref) ⇒ handlers.find(_._2 == ref.path) match {
      case Some((id, path)) ⇒
        handlers.remove(id)
      case None ⇒ warning(log, "could not clean queryId of actor in {}", ref.path)
    }
      log.debug("handler terminated. living handlers: {}", handlers)

    case TokenValidationResult(Failure(e), _, handler, _) ⇒
      handler ! DoneWithQueryExecution.error(e)

    case TokenValidationResult(Success(user), query, handler, clientAddress) ⇒
      prepareMaterialization(handler, query, Some(user), clientAddress)

    case NewQuery(query, clientAddress) ⇒
      debug(log, "client from {} posted new query {}", clientAddress, query)
      val handler = sender()

      query.authToken match {
        case Some(token) ⇒

          trace(log, query.traceId.get, ValidateToken,
            Variation.Attempt, "sending token {} for validation", token)

          authService.ask(
            AuthenticationActor.ValidateToken(token, query.traceId.get))(authenticationTimeout)
            .mapTo[Try[ApiUser]]
            .map(tu ⇒ TokenValidationResult(tu, query, handler, clientAddress))
            .andThen {
              case Success(res) ⇒
                trace(log, query.traceId.get, ValidateToken, Variation.Success,
                  "validated token {} for user {}", token, res.user)
              case Failure(e) ⇒
                trace(log, query.traceId.get, ValidateToken, Variation.Failure(e),
                  "token validation for token {} failed", token)
            }.pipeTo(self)

        case None ⇒ prepareMaterialization(handler, query, None, clientAddress)
      }

    case m ⇒ warning(log, "oops! It looks like I received the wrong message: {}", m)

  }
}

object SonicController {

  case class NewQuery(query: Query, clientAddress: Option[InetAddress])

  class UnauthorizedException(user: Option[ApiUser], clientAddress: Option[InetAddress])
    extends Exception(user.map(u ⇒ s"user ${u.user} is unauthorized " +
      s"to access this source from ${clientAddress.getOrElse("unknown address")}")
      .getOrElse(s"unauthenticated user cannot access this source from ${clientAddress.getOrElse("unknown address")}"))

  case object GetHandlers

}
