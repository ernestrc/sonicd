package build.unstable.sonicd.system.actor

import java.net.InetAddress

import akka.actor.SupervisorStrategy.Restart
import akka.actor._
import akka.pattern._
import akka.util.Timeout
import build.unstable.sonic.JsonProtocol._
import build.unstable.sonic.model._
import build.unstable.sonicd.SonicdLogging
import build.unstable.tylog.Variation
import org.slf4j.MDC
import org.slf4j.event.Level
import spray.json._

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

class SonicdController(authService: ActorRef, authenticationTimeout: Timeout) extends Actor with SonicdLogging {

  import SonicdController._

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    log.info("starting Sonic Controller {}", self.path)
    loadSourceConfiguration()
  }

  @throws[Exception](classOf[Exception])
  override def postRestart(reason: Throwable): Unit = {
    loadSourceConfiguration()
    log.error(reason, "RESTARTED CONTROLLER")
  }

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(loggingEnabled = true) {
    case NonFatal(e) ⇒ Restart
  }

  import context.dispatcher


  /* HELPERS */
  def loadSourceConfiguration(): Unit = {
    // TODO
    ???
  }

  def getSourceClass(sonicdSourceClass: String): Try[Class[_]] = {
    val clazzLoader = this.getClass.getClassLoader
    Try(clazzLoader.loadClass("build.unstable.sonicd.source." + sonicdSourceClass))
      .orElse(Try(clazzLoader.loadClass(sonicdSourceClass)))
      .orElse(Try(clazzLoader.loadClass("build.unstable.sonic.server.source." + sonicdSourceClass)))
  }

  // query config can come in two flavours:
  // - as a JSON object, in which case it will be passed to the underlying source untouched
  // - as a string, in which case it needs to be resolved from classpath resources (sources.json)
  def resolveQueryConfig(id: Long, q: Query): Query = q.config match {
    case o: JsObject ⇒ q.copy(query_id = Some(id))
    case JsString(alias) ⇒ try {
      new Query(Some(id), q.traceId, q.auth, q.query, sources(alias))
    } catch {
      case e: Exception ⇒ throw new Exception(s"could not load query config '$alias'", e)
    }
    case _ ⇒
      throw new Exception("'config' key in query config can only be either a full config " +
        "object or an alias (string) that will be resolved from sonicd's configuration")
  }

  def prepareMaterialization(handler: ActorRef, query: Query,
                             user: Option[ApiUser], clientAddress: Option[InetAddress]): Unit = {
    try {
      handled += 1L
      val queryId = handled

      val resolvedQuery = resolveQueryConfig(handled, query)
      val resolvedConfig = resolvedQuery.config.asJsObject

      val sonicdSourceClass = resolvedConfig.fields.getOrElse("class",
        throw new Exception(s"missing key 'class' in config")).convertTo[String]

      val source = getSourceClass(sonicdSourceClass)
        .getOrElse(throw new Exception(s"could not find $sonicdSourceClass in the classpath"))
        .getConstructors()(0)
        .newInstance(resolvedQuery, context, RequestContext(resolvedQuery.traceId.get, user)).asInstanceOf[DataSource]

      log.debug("successfully instantiated source {} for query with id '{}'", sonicdSourceClass, queryId)

      val securityLevel = resolvedConfig.fields.get("security").map(_.convertTo[Int])

      if (isAuthorized(user, securityLevel, clientAddress)) {
        handler ! source.publisher
      } else handler ! Failure(new UnauthorizedException(user, clientAddress))
    } catch {
      case e: Exception ⇒
        log.error(e, "error when preparing stream materialization")
        handler ! Failure(e)
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

  //TODO deprecate queryId
  var handled: Long = 0L
  val sources = mutable.Map.empty[String, JsObject]

  /* BEHAVIOUR */

  override def receive: Receive = {

    case TokenValidationResult(f@Failure(e), query, handler, _) ⇒
      log.tylog(Level.INFO, query.traceId.get, AuthenticateUser, Variation.Failure(e), "token validation failed")
      handler ! f

    case TokenValidationResult(Success(user), query, handler, clientAddress) ⇒
      try {
        MDC.put("user", user.user)
        MDC.put("mode", user.mode.toString)
        log.tylog(Level.INFO, query.traceId.get, AuthenticateUser, Variation.Success, "validated token successfully")
        prepareMaterialization(handler, query, Some(user), clientAddress)
      } catch {
        case e: Exception ⇒
          log.error(e, "error when preparing stream materialization")
          handler ! Failure(e)
      }

    case NewCommand(a: Authenticate, _) ⇒ authService forward a

    case NewCommand(query: Query, clientAddress) ⇒
      log.debug("client from {} posted new query {}", clientAddress, query)
      val handler = sender()

      log.tylog(Level.INFO, query.traceId.get, AuthenticateUser,
        Variation.Attempt, "authenticating with auth: {}", query.auth)

      query.auth match {
        case Some(SonicdAuth(token)) ⇒
          authService.ask(
            ValidateToken(token, query.traceId.get))(authenticationTimeout)
            .mapTo[Try[ApiUser]]
            .map(tu ⇒ TokenValidationResult(tu, query, handler, clientAddress))
            .recover {
              case e: Exception ⇒ TokenValidationResult(Failure(e), query, handler, clientAddress)
            }.pipeTo(self)

        // if no provider is passed, then use sonicds auth
        case Some(auth) ⇒
          val providerClass = auth.provider
          val result = try {
            val provider = providerClass.getConstructors()(0).newInstance().asInstanceOf[ExternalAuthProvider]
            provider.validate(auth, context.system, query.traceId.get)
              .map(tu ⇒ TokenValidationResult(Success(tu), query, handler, clientAddress))
          } catch {
            case e: Exception ⇒ Future.failed(e)
          }

          result.recover {
            case e: Exception ⇒ TokenValidationResult(Failure(e), query, handler, clientAddress)
          }.pipeTo(self)

        case None ⇒
          log.tylog(Level.INFO, query.traceId.get, AuthenticateUser, Variation.Success, "user presented no auth token")
          prepareMaterialization(handler, query, None, clientAddress)
      }

    case m ⇒ log.warning("oops! It looks like I received the wrong message: {}", m)

  }
}

object SonicdController {

  class UnauthorizedException(user: Option[ApiUser], clientAddress: Option[InetAddress])
    extends Exception(user.map(u ⇒ s"user ${u.user} is unauthorized " +
      s"to access this source from ${clientAddress.getOrElse("unknown address")}")
      .getOrElse(s"unauthenticated user cannot access this source from ${clientAddress.getOrElse("unknown address")}. Please login first"))

  case class TokenValidationResult(user: Try[ApiUser], query: Query,
                                   handler: ActorRef, clientAddress: Option[InetAddress])

}
