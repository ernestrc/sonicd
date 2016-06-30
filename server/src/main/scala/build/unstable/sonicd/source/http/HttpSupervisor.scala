package build.unstable.sonicd.source.http

import akka.actor.{Actor, ActorRef, Status, Terminated}
import akka.http.scaladsl.model._
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.pattern._
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import build.unstable.sonicd.Sonicd
import build.unstable.sonicd.model.JsonProtocol._
import build.unstable.sonicd.model.SonicdLogging
import build.unstable.sonicd.source.http.HttpSupervisor._
import build.unstable.tylog.Variation
import spray.json._

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

object HttpSupervisor {

  class HttpException(msg: String, code: String) extends Exception(s"$code: $msg")

  case class HttpRequestCommand(traceId: String, request: HttpRequest)

  trait Identifiable {
    def id: String

    def setTraceId(newId: String): Identifiable
  }

}

abstract class HttpSupervisor[T <: Identifiable] extends Actor with SonicdLogging {

  import context.dispatcher

  /* ABSTRACT */

  def cancelRequestFromResult(t: T): Option[HttpRequest]

  def extraHeaders: scala.collection.immutable.Seq[HttpHeader]

  def masterUrl: String

  def port: Int

  def poolSettings: ConnectionPoolSettings

  def httpEntityTimeout: FiniteDuration

  def jsonFormat: RootJsonFormat[T]


  /* HELPERS */

  case class HttpCommandSuccess(t: Identifiable)

  def to[S: JsonFormat](traceId: String)(response: HttpResponse): Future[S] = {
    trace(log, traceId, DownloadHttpBody, Variation.Attempt,
      "download http body of response with status {} headers {}", response._1, response._2)
    response.entity.toStrict(httpEntityTimeout)
      .map(_.data.decodeString("UTF-8"))
      .andThen {
        case Success(body) ⇒
          trace(log, traceId, DownloadHttpBody, Variation.Success, "decoded utf8 body: {}", body)
          trace(log, traceId, ParseHttpBody, Variation.Attempt, "")
        case Failure(e) ⇒ trace(log, traceId, DownloadHttpBody, Variation.Failure(e), "failed to download entity")
      }.map(_.parseJson.convertTo[S]).andThen {
      case Success(parsed) ⇒ trace(log, traceId, ParseHttpBody, Variation.Success, "parsed http body")
      case Failure(e) ⇒ trace(log, traceId, ParseHttpBody, Variation.Attempt,
        "error when parsing http body with {}", format.getClass)
    }
  }

  def doRequest[S: JsonFormat](traceId: String, request: HttpRequest)
                              (mapSuccess: (String) ⇒ (HttpResponse) ⇒ Future[S]): Future[S] = {
    trace(log, traceId, HttpReq(request.method.value), Variation.Attempt,
      "sending {} http request to {}{}", request.method.value, masterUrl, request._2)
    Source.single(request.copy(headers = extraHeaders) → traceId)
      .via(connectionPool)
      .runWith(Sink.head)
      .flatMap {
        case t@(Success(response), _) if response.status.isSuccess() =>
          trace(log, traceId, HttpReq(request.method.value), Variation.Success, "")
          mapSuccess(traceId)(response)
        case (Success(response), _) ⇒
          trace(log, traceId, HttpReq(request.method.value), Variation.Success,
            "http request succeded but response status is {}", response._1)
          val parsed = response.entity.toStrict(httpEntityTimeout)
          parsed.recoverWith {
            case e: Exception ⇒
              val er = new HttpException(s"request to $masterUrl failed unexpectedly", response.status.value)
              error(log, er, s"failed parsing entity from failed request")
              Future.failed(er)
          }.flatMap { en ⇒
            val entityMsg = en.data.decodeString("UTF-8")
            val er = new HttpException(entityMsg, response.status.value)
            error(log, er, "unsuccessful response from server")
            Future.failed(er)
          }
        case (Failure(e), _) ⇒
          trace(log, traceId, HttpReq(request.method.value),
            Variation.Failure(e), "http request failed ")
          Future.failed(e)
      }
  }

  def runStatement(traceId: String, request: HttpRequest): Future[Identifiable] = {

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

  val queries = scala.collection.mutable.Map.empty[ActorRef, Identifiable]


  /* BEHAVIOUR */

  override def receive: Actor.Receive = {

    case Terminated(ref) ⇒
      queries.remove(ref).map { res ⇒
        val traceId = res.id
        cancelRequestFromResult(res.asInstanceOf[T]).map { cancelUri ⇒
          cancelQuery(traceId, cancelUri).andThen {
            case Success(wasCanceled) if wasCanceled ⇒ log.debug("successfully canceled query '{}'", traceId)
            case s: Success[_] ⇒ log.error("could not cancel query '{}': DELETE response was not 200 OK", traceId)
            case Failure(e) ⇒ error(log, e, "error canceling query '{}'", traceId)
          }
        }.getOrElse(warning(log, "could not cancel query '{}': paritalCancelUri is empty", ref))
      }.getOrElse(log.debug("could not remove query of publisher: {}: not queryresults found", ref))

    case HttpCommandSuccess(r) ⇒
      val pub = sender()
      queries.update(pub, r)
      log.debug("extracted query results for query '{}'", r.id)
      pub ! r

    case HttpRequestCommand(traceId, s) ⇒
      debug(log, "{} supervising query '{}'", self.path, s)
      val pub = sender()
      context.watch(pub)
      runStatement(traceId, s)
        .map(s ⇒ HttpCommandSuccess(s.setTraceId(traceId))).pipeTo(self)(pub)

    case f: Status.Failure ⇒ sender() ! f

    case anyElse ⇒ warning(log, "recv unexpected msg: {}", anyElse)
  }
}
