package build.unstable.sonicd.api

/*
import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import akka.util.Timeout
import akka.pattern.ask
import build.unstable.sonic.server.http.EndpointUtils
import build.unstable.sonicd.source.SourceConfig
import build.unstable.sonicd.system.actor.SonicdController

class SourcesEndpoint(controller: ActorRef, responseTimeout: Timeout, actorTimeout: Timeout)
                     (implicit val mat: ActorMaterializer, system: ActorSystem) {
  import SourceConfig._

  implicit val t: Timeout = responseTimeout

  val route: Route = path("source") {
    put {
      entity(as[SourceConfig]){
        complete {
          controller.ask(SonicdController.PutSource()).mapTo[String]
        }
      }
    }
  }
}*/
