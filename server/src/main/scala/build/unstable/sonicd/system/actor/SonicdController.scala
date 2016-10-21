package build.unstable.sonicd.system.actor

import java.net.InetAddress

import akka.actor.SupervisorStrategy.Restart
import akka.actor._
import akka.pattern._
import akka.util.Timeout
import build.unstable.sonic.JsonProtocol._
import build.unstable.sonic.cluster.RequestEndpoint
import build.unstable.sonic.model._
import build.unstable.sonicd.SonicdLogging
import build.unstable.tylog.Variation
import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import org.slf4j.MDC
import org.slf4j.event.Level
import spray.json._

import scala.concurrent.Future
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

class SonicdController(authService: ActorRef, authenticationTimeout: Timeout) extends Actor with SonicdLogging {

  import SonicdController._

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    log.info("starting Sonic Controller {}", self.path)
  }

  @throws[Exception](classOf[Exception])
  override def postRestart(reason: Throwable): Unit = {
    log.error(reason, "RESTARTED CONTROLLER")
  }

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(loggingEnabled = true) {
    case NonFatal(e) ⇒ Restart
  }

  import context.dispatcher


  /* HELPERS */

  def getSourceClass(query: Query): Try[Class[_]] = {
    val clazzLoader = this.getClass.getClassLoader

    Try(clazzLoader.loadClass(query.sonicdSourceClass))
      .orElse(Try(clazzLoader.loadClass("build.unstable.sonic.server.source." + query.sonicdSourceClass)))
      .orElse(Try(clazzLoader.loadClass("build.unstable.sonicd.source." + query.sonicdSourceClass)))
  }

  def prepareMaterialization(handler: ActorRef, q: Query,
                             user: Option[ApiUser], clientAddress: Option[InetAddress]): Unit = {
    try {
      handled += 1L
      val queryId = handled
      val query = q.copy(query_id = Some(queryId))
      val source = getSourceClass(query)
        .getOrElse(throw new Exception(s"could not find ${query.sonicdSourceClass} in the classpath")).getConstructors()(0)
        .newInstance(query, context, RequestContext(query.traceId.get, user)).asInstanceOf[DataSource]

      log.debug("successfully instantiated source {} for query with id '{}'", source, queryId)

      val securityLevel = query.sonicdConfig.fields.get("security").map(_.convertTo[Int])

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

  case class TokenValidationResult(user: Try[ApiUser], query: Query,
                                   handler: ActorRef, clientAddress: Option[InetAddress])

  /* BEHAVIOUR */

  override def receive: Receive = {
    case RequestEndpoint(address) ⇒

    case TokenValidationResult(f@Failure(e), query, handler, _) ⇒
      log.tylog(Level.INFO, query.traceId.get, AuthenticateUser, Variation.Failure(e), "token validation failed")
      handler ! f

    case TokenValidationResult(Success(user), query, handler, clientAddress) ⇒
      try {
        MDC.put("user", user.user)
        MDC.put("mode", user.mode.toString)
        MDC.put("source", query.sonicdSourceClass)
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

  implicit class SonicdQuery(query: Query) {

    //CAUTION: leaking this value outside of sonicd-server is a major security risk
    private[unstable] lazy val sonicdConfig: JsObject = query.config match {
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

    private[unstable] lazy val sonicdSourceClass: String = sonicdConfig.fields.getOrElse("class",
      throw new Exception(s"missing key 'class' in config")).convertTo[String]

  }

  class UnauthorizedException(user: Option[ApiUser], clientAddress: Option[InetAddress])
    extends Exception(user.map(u ⇒ s"user ${u.user} is unauthorized " +
      s"to access this source from ${clientAddress.getOrElse("unknown address")}")
      .getOrElse(s"unauthenticated user cannot access this source from ${clientAddress.getOrElse("unknown address")}. Please login first"))

}
