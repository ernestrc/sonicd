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
import org.slf4j.LoggerFactory

import scala.concurrent.Future

class QueryEndpoint(controller: ActorRef, responseTimeout: Timeout, actorTimeout: Timeout)
                   (implicit mat: ActorMaterializer, system: ActorSystem) extends CorsDirectives {

  import JsonProtocol._
  import akka.stream.scaladsl._
  import build.unstable.sonicd.model._

  implicit val t: Timeout = responseTimeout

  val log = LoggerFactory.getLogger(this.getClass.getSimpleName)

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
  def messageSerDe: Flow[Message, Message, Any] = {
    Flow.fromGraph(GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._

      val deserialize = Flow[Message].map {
        case b: BinaryMessage.Strict ⇒ SonicMessage.fromBinary(b)
        case m: TextMessage.Strict ⇒ SonicMessage.fromJson(m.text)
        case msg ⇒ throw new Exception(s"invalid msg: $msg")
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
            extractUpgradeToWebSocket { upgrade ⇒
              complete {
                upgrade.handleMessages(messageSerDe.recover {
                  case e: Exception ⇒ TextMessage(DoneWithQueryExecution.error(e).json.toString())
                })
              }
            }
          }
        }
      }
    } ~ get {
      path("consume" / Segment) { streamId ⇒
        parameterMap { params ⇒
          complete {
            Future.failed(new Exception("not implemented yet"))
          }
        }
      }
    } ~ get {
      path(Segments) { fold ⇒
        getFromResource(fold.reduce(_ + "/" + _))
      }
    }
}

