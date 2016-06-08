package build.unstable.sonicd.api

import java.util.UUID

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive1, RouteResult}
import akka.stream.ActorMaterializer
import build.unstable.sonicd.model.SonicdLogging

import scala.util.{Failure, Success}

trait RouteLogging extends SonicdLogging {

  def mat: ActorMaterializer

  def extractTraceHeader: Directive1[Option[String]] = {
    extractRequest.tflatMap {
      case Tuple1(req) ⇒
        provide(req.headers.find(_.name() == "X-TRACE").map(_.value()))
    }
  }

  def instrument(callType: CallType, traceIdMaybe: Option[String]): Directive1[String] = {
    val id = traceIdMaybe.getOrElse(UUID.randomUUID().toString)
    trace(log, id, callType, Variation.Attempt)

    mapRouteResultFuture {
      _.andThen {
        case Success(r: RouteResult.Complete) ⇒
          trace(log, id, callType, Variation.Success)
        case Success(r@RouteResult.Rejected(rejections)) ⇒
          val e = new Exception(rejections.foldLeft("")((acc, r) ⇒ acc + "; " + r.toString))
          trace(log, id, callType, Variation.Failure(e))
        case Failure(e) ⇒
          trace(log, id, callType, Variation.Failure(e))
      }(mat.executionContext)
    }.tflatMap(_ ⇒ provide(id))
  }
}
