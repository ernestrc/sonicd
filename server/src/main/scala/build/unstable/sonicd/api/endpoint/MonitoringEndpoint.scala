package build.unstable.sonicd.api.endpoint

import akka.actor.{ActorPath, ActorRef, ActorSystem}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.pattern._
import akka.stream.ActorMaterializer
import akka.util.Timeout
import build.unstable.sonicd.BuildInfo
import build.unstable.sonicd.api.EndpointUtils
import build.unstable.sonicd.model._
import build.unstable.sonicd.system.actor.SonicController
import spray.json._
import JsonProtocol._

class MonitoringEndpoint(responseTimeout: Timeout, controller: ActorRef)
                        (implicit val mat: ActorMaterializer, system: ActorSystem) extends EndpointUtils {

  import mat.executionContext

  implicit val t: Timeout = responseTimeout

  val route: Route = path("version") {
    get {
      instrumentRoute(HandleGetHandlers) { traceId ⇒
        complete {
          s"${BuildInfo.version} (${BuildInfo.commit} ${BuildInfo.builtAt})"
        }
      }
    }
  } ~ path("handlers") {
    instrumentRoute(HandleGetHandlers) { traceId ⇒
      complete {
        controller.ask(SonicController.GetHandlers)
          .mapTo[Map[ActorPath, String]].map(m ⇒ JsObject(m.map(kv ⇒ kv._1.toString → JsString(kv._2))))
      }
    }
  }
}
