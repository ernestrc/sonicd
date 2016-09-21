package build.unstable.sonicd.source.http

import akka.actor.{Actor, ActorRef, Status, Terminated}
import akka.http.scaladsl.model._
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.pattern._
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import build.unstable.sonic.JsonProtocol._
import build.unstable.sonicd.source.http.HttpSupervisor._
import build.unstable.sonicd.{Sonicd, SonicdLogging}
import build.unstable.tylog.Variation
import org.slf4j.event.Level
import spray.json._

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

object HttpSupervisor {

  class HttpException(msg: String, code: String) extends Exception(s"$code: $msg")

  case class HttpRequestCommand(traceId: String, request: HttpRequest)

  trait Traceable {
    def id: String

    def setTraceId(newId: String): Traceable
  }

}

abstract class HttpSupervisor[T <: Traceable] extends Actor with SonicdLogging {

  import context.dispatcher

  /* ABSTRACT */

  def cancelRequestFromResult(t: T): Option[HttpRequest]

  def extraHeaders: scala.collection.immutable.Seq[HttpHeader]

  def masterUrl: String

  def port: Int

  def poolSettings: ConnectionPoolSettings

  def httpEntityTimeout: FiniteDuration

  def jsonFormat: RootJsonFormat[T]

  //debug flag to output raw payloads
  def debug: Boolean


  /* HELPERS */

  case class HttpCommandSuccess(t: Traceable)

  def to[S: JsonFormat](traceId: String)(response: HttpResponse): Future[S] = {
    log.tylog(Level.DEBUG, traceId, DownloadHttpBody, Variation.Attempt,
      "download http body of response with status {} headers {}", response._1, response._2)
    response.entity.toStrict(httpEntityTimeout)
      .map(_.data.decodeString("UTF-8"))
      .andThen {
        case Success(body) ⇒
          val msg = if (debug) s"decoded utf8 body: $body" else ""
          log.tylog(Level.DEBUG, traceId, DownloadHttpBody, Variation.Success, msg)
          log.tylog(Level.DEBUG, traceId, ParseHttpBody, Variation.Attempt, "")
        case Failure(e) ⇒ log.tylog(Level.DEBUG, traceId, DownloadHttpBody, Variation.Failure(e), "failed to download entity")
      }.map(_.parseJson.convertTo[S]).andThen {
      case Success(parsed) ⇒ log.tylog(Level.DEBUG, traceId, ParseHttpBody, Variation.Success, "parsed http body")
      case Failure(e) ⇒ log.tylog(Level.DEBUG, traceId, ParseHttpBody, Variation.Attempt,
        "error when parsing http body with {}", format.getClass)
    }
  }

  def doRequest[S: JsonFormat](traceId: String, request: HttpRequest)
                              (mapSuccess: (String) ⇒ (HttpResponse) ⇒ Future[S]): Future[S] = {
    log.tylog(Level.DEBUG, traceId, HttpReq(request.method.value, masterUrl), Variation.Attempt,
      "sending {} http request to {}{}", request.method.value, masterUrl, request._2)
    Source.single(request.copy(headers = request.headers ++: extraHeaders) → traceId)
      .via(connectionPool)
      .runWith(Sink.head)
      .flatMap {
        case t@(Success(response), _) if response.status.isSuccess() =>
          log.tylog(Level.DEBUG, traceId, HttpReq(request.method.value, masterUrl), Variation.Success, "")
          mapSuccess(traceId)(response)
        case (Success(response), _) ⇒
          log.tylog(Level.DEBUG, traceId, HttpReq(request.method.value, masterUrl), Variation.Success,
            "http request succeded but response status is {}", response._1)
          val parsed = response.entity.toStrict(httpEntityTimeout)
          parsed.recoverWith {
            case e: Exception ⇒
              val er = new HttpException(s"request to $masterUrl failed unexpectedly", response.status.value)
              log.error(er, s"failed parsing entity from failed request")
              Future.failed(er)
          }.flatMap { en ⇒
            val entityMsg = en.data.decodeString("UTF-8")
            val er = new HttpException(entityMsg, response.status.value)
            log.error(er, "unsuccessful response from server")
            Future.failed(er)
          }
        case (Failure(e), _) ⇒
          log.tylog(Level.DEBUG, traceId, HttpReq(request.method.value, masterUrl),
            Variation.Failure(e), "http request failed ")
          Future.failed(e)
      }
  }

  def runStatement(traceId: String, request: HttpRequest): Future[Traceable] = {

    doRequest(traceId, request)(to[T]).map(_.setTraceId(traceId))
  }

  def cancelQuery(traceId: String, request: HttpRequest): Future[Boolean] =
    doRequest(traceId, request) { traceId ⇒ resp ⇒
      Future.successful(resp.status.isSuccess())
    }

  implicit val format: RootJsonFormat[T] = jsonFormat
  implicit val materializer: ActorMaterializer =
    ActorMaterializer(ActorMaterializerSettings(context.system))

  lazy val connectionPool = Sonicd.http.newHostConnectionPool[String](
    host = masterUrl,
    port = port,
    settings = poolSettings
  )


  /* STATE */

  val queries = scala.collection.mutable.Map.empty[ActorRef, Traceable]


  /* BEHAVIOUR */

  override def receive: Actor.Receive = {

    case Terminated(ref) ⇒
      queries.remove(ref).map { res ⇒
        val traceId = res.id
        cancelRequestFromResult(res.asInstanceOf[T]).map { cancelUri ⇒
          cancelQuery(traceId, cancelUri).andThen {
            case Success(wasCanceled) if wasCanceled ⇒ log.debug("successfully canceled query '{}'", traceId)
            case s: Success[_] ⇒ log.warning("could not cancel query {}: response was not 200 OK: {}", traceId, s.value)
            case Failure(e) ⇒ log.error(e, "error canceling query '{}'", traceId)
          }
        }
      }.getOrElse(log.debug("could not remove query of publisher: {}: not queryresults found", ref))

    case HttpCommandSuccess(r) ⇒
      val pub = sender()
      queries.update(pub, r)
      log.debug("extracted query results for query '{}'", r.id)
      pub ! r

    case HttpRequestCommand(traceId, s) ⇒
      log.debug("{} supervising query '{}'", self.path, s)
      val pub = sender()
      context.watch(pub)
      runStatement(traceId, s)
        .map(s ⇒ HttpCommandSuccess(s.setTraceId(traceId))).pipeTo(self)(pub)

    case f: Status.Failure ⇒ sender() ! f

    case anyElse ⇒ log.warning("recv unexpected msg: {}", anyElse)
  }
}
