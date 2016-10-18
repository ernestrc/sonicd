package build.unstable.sonic.server.http

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.http.scaladsl.model.RemoteAddress
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.http.scaladsl.server.Directives._
import akka.stream._
import akka.stream.actor.{ActorPublisher, ActorSubscriber}
import akka.stream.scaladsl._
import akka.util.Timeout
import build.unstable.sonic.model.{SonicMessage, StreamCompleted}
import build.unstable.sonic.server.system.WsHandler
import ch.megard.akka.http.cors.CorsDirectives

class QueryEndpoint(controller: ActorRef, authService: ActorRef, responseTimeout: Timeout, actorTimeout: Timeout)
                   (implicit val mat: ActorMaterializer, system: ActorSystem)
  extends CorsDirectives with EndpointUtils {

  implicit val t: Timeout = responseTimeout

  def wsFlowHandler(clientAddress: RemoteAddress): Flow[SonicMessage, SonicMessage, Any] = {

    val wsHandler = system.actorOf(Props(classOf[WsHandler], controller, authService, clientAddress.toOption))
    Flow.fromSinkAndSource[SonicMessage, SonicMessage](
      Sink.fromSubscriber(ActorSubscriber(wsHandler)),
      Source.fromPublisher[SonicMessage](ActorPublisher(wsHandler))
    ).recover {
      case e: Exception ⇒ StreamCompleted.error("no-trace-id", e)
    }
  }

  /** Transforms flow by prepending a deserialization step and
    * appending a serialization step to integrate it with the
    * ws api.
    *
    * {{{
    *           +------------------------------------------+
    *           |                Resulting Flow            |
    *           |                                          |
    *           |  +------+       +------+       +------+  |
    *           |  |      | Event |      | Event |      |  |
    * Message ~~>  |  de  |  ~~>  | flow |  ~~>  | ser  |  ~~> Message
    *           |  |      |       |      |       |      |  |
    *           |  +------+       +------+       +------+  |
    *           +------------------------------------------+
    * }}}
    */
  def messageSerDe(clientAddress: RemoteAddress): Flow[Message, Message, Any] = {
    Flow.fromGraph(GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._

      val deserialize = Flow[Message].flatMapConcat {
        case t: TextMessage.Strict ⇒
          Source.single(t).via(Flow[TextMessage.Strict].map(t ⇒ SonicMessage.fromJson(t.text)))
        case t: TextMessage.Streamed ⇒
          t.textStream.via(Flow[String].fold("")((acc, s) ⇒ acc + s).map(SonicMessage.fromJson))
        case msg ⇒ throw new Exception(s"invalid msg: $msg")
      }

      val serialize = Flow[SonicMessage].map(_.toWsMessage)

      val de = b.add(deserialize)
      val ser = b.add(serialize)
      val eventFlow = b.add(wsFlowHandler(clientAddress))

      de ~> eventFlow ~> ser

      FlowShape(de.in, ser.out)
    })
  }

  val route =
    get {
      path("query") {
        pathEndOrSingleSlash {
          cors() {
            instrumentRoute(HandleExtractWebSocketUpgrade, None) { _ ⇒
              extractClientIP { ip ⇒
                extractUpgradeToWebSocket { upgrade ⇒
                  complete {
                    upgrade.handleMessages(messageSerDe(ip).recover {
                      case e: Exception ⇒ TextMessage(StreamCompleted.error("no-trace-id", e).json.toString())
                    })
                  }
                }
              }
            }
          }
        }
      }
    }
}

