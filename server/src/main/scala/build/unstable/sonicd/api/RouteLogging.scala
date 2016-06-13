package build.unstable.sonicd.api

import java.util.UUID

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive1, RouteResult}
import akka.stream.ActorMaterializer
import build.unstable.sonicd.model.SonicdLogging
import build.unstable.tylog.Variation

trait RouteLogging extends SonicdLogging {

  def mat: ActorMaterializer

  def extractTraceHeader: Directive1[Option[TraceID]] = {
    extractRequest.tflatMap {
      case Tuple1(req) ⇒
        provide(req.headers.find(_.name() == "X-TRACE").map(_.value()))
    }
  }

  def instrumentRoute(callType: CallType, traceIdMaybe: Option[TraceID]): Directive1[TraceID] = {

    //generate request_id if traceId was extracted from header
    //otherwise generate one and use it for both
    val traceId = traceIdMaybe.getOrElse(UUID.randomUUID().toString)

    trace(log, traceId, callType, Variation.Attempt, "")

    mapRouteResultPF {
      case r: RouteResult.Complete ⇒
        trace(log, traceId, callType, Variation.Success, "")
        r
      case r@RouteResult.Rejected(rejections) ⇒
        val e = new Exception(rejections.foldLeft("")((acc, r) ⇒ acc + "; " + r.toString))
        trace(log, traceId, callType, Variation.Failure(e), "")
        r
    }.tflatMap { _ ⇒
      provide(traceId)
    }
  }

  def instrumentRoute(callType: CallType): Directive1[TraceID] = {
    extractTraceHeader.tflatMap { case Tuple1(traceIdMaybe) ⇒
      instrumentRoute(callType, traceIdMaybe)
    }
  }
}
