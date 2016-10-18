package build.unstable.sonic.server.http

import java.util.UUID

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive1, RouteResult}
import build.unstable.sonic.server.ServerLogging
import build.unstable.tylog.Variation
import org.slf4j.event.Level

trait EndpointUtils extends ServerLogging {

  def extractTraceHeader: Directive1[Option[TraceID]] = {
    extractRequest.tflatMap {
      case Tuple1(req) ⇒
        provide(req.headers.find(_.name() == "X-TRACE").map(_.value()))
    }
  }

  // should take Level when fixed: https://github.com/ernestrc/tylog/issues/1
  def instrumentRoute(callType: CallType, traceIdMaybe: Option[TraceID]): Directive1[TraceID] = {

    val traceId = traceIdMaybe.getOrElse(UUID.randomUUID().toString)

    log.tylog(Level.DEBUG, traceId, callType, Variation.Attempt, "")

    mapRouteResultPF {
      case r: RouteResult.Complete ⇒
        log.tylog(Level.DEBUG, traceId, callType, Variation.Success, "")
        r
      case r@RouteResult.Rejected(rejections) ⇒
        val e = new Exception(rejections.foldLeft("")((acc, r) ⇒ acc + "; " + r.toString))
        log.tylog(Level.DEBUG, traceId, callType, Variation.Failure(e), "")
        r
    }.tflatMap { _ ⇒
      provide(traceId)
    }
  }

  def instrumentRoute(level: Level, callType: CallType): Directive1[TraceID] = {
    extractTraceHeader.tflatMap { case Tuple1(traceIdMaybe) ⇒
      instrumentRoute(callType, traceIdMaybe)
    }
  }
}
