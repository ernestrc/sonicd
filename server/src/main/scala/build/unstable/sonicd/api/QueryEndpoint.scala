package build.unstable.sonicd.api

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage}
import akka.http.scaladsl.server.Directives._
import akka.stream._
import akka.stream.actor.{ActorPublisher, ActorSubscriber}
import akka.util.Timeout
import build.unstable.sonicd.model.JsonProtocol
import build.unstable.sonicd.system.WsHandler
import ch.megard.akka.http.cors.CorsDirectives

import scala.concurrent.Future

class QueryEndpoint(controller: ActorRef, responseTimeout: Timeout, actorTimeout: Timeout)
                   (implicit val mat: ActorMaterializer, system: ActorSystem)
  extends CorsDirectives with RouteLogging {

  import JsonProtocol._
  import akka.stream.scaladsl._
  import build.unstable.sonicd.model._

  implicit val t: Timeout = responseTimeout

  def wsFlowHandler: Flow[SonicMessage, SonicMessage, Any] = {

    val wsHandler = system.actorOf(Props(classOf[WsHandler], controller))
    Flow.fromSinkAndSource[SonicMessage, SonicMessage](
    Sink.fromSubscriber(ActorSubscriber(wsHandler)),
    Source.fromPublisher[SonicMessage](ActorPublisher(wsHandler))
    ).recover {
      case e: Exception ⇒ DoneWithQueryExecution.error(e)
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
  def messageSerDe(traceId: String): Flow[Message, Message, Any] = {
    Flow.fromGraph(GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._

      val deserialize = Flow[Message].map {
        case b: BinaryMessage.Strict ⇒ SonicMessage.fromBinary(b)
        case m: TextMessage.Strict ⇒ SonicMessage.fromJson(m.text)
        case msg ⇒ throw new Exception(s"invalid msg: $msg")
      }.map {
        case q: Query ⇒ q.copy(query_id = traceId)
        case anyOther ⇒ anyOther
      }

      val serialize = Flow[SonicMessage].map(_.toWsMessage)

      val de = b.add(deserialize)
      val ser = b.add(serialize)
      val eventFlow = b.add(wsFlowHandler)

      de ~> eventFlow ~> ser

      FlowShape(de.in, ser.out)
    })
  }

  val route =
    get {
      path("query") {
        pathEndOrSingleSlash {
          cors() {
            //first protocol in list is used as trace_id
            extractOfferedWsProtocols { protocols ⇒
              instrument(HandleExtractWebSocketUpgrade, protocols.headOption) { traceId ⇒
                extractUpgradeToWebSocket { upgrade ⇒
                  complete {
                    upgrade.handleMessages(messageSerDe(traceId).recover {
                      case e: Exception ⇒ TextMessage(DoneWithQueryExecution.error(e).json.toString())
                    })
                  }
                }
              }
            }
          }
        }
      }
    } ~ get {
      path("subscribe" / Segment) { streamId ⇒
        extractTraceHeader { traceIdMaybe ⇒
          instrument(HandleSubscribe, traceIdMaybe) { traceId ⇒
            parameterMap { params ⇒
              complete {
                Future.failed(new Exception("not implemented yet"))
              }
            }
          }
        }
      }
    }
}

