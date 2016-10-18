package build.unstable.sonic.client

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.UUID

import akka.actor.{ActorSystem, Cancellable, Props}
import akka.stream.ActorMaterializer
import akka.stream.actor.ActorPublisher
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.util.ByteString
import build.unstable.sonic.model._
import spray.json.{JsArray, JsString}

import scala.concurrent.Future

object Sonic {

  private[unstable] def sonicSupervisorProps(addr: InetSocketAddress): Props =
    Props(classOf[SonicSupervisor], addr)

  //length prefix framing
  private[unstable] def lengthPrefixEncode(bytes: ByteString): ByteString = {
    val len = ByteBuffer.allocate(4)
    len.putInt(bytes.length)
    ByteString(len.array() ++ bytes)
  }

  private val fold = Sink.fold[Vector[SonicMessage], SonicMessage](Vector.empty[SonicMessage])((a, e) ⇒ a :+ e)

  case class Client(address: InetSocketAddress)(implicit system: ActorSystem) {

    val supervisor = system.actorOf(Sonic.sonicSupervisorProps(address))

    type Token = String


    final def stream(cmd: SonicCommand)(implicit system: ActorSystem): Source[SonicMessage, Cancellable] = {

      val publisher: Props = Props(classOf[SonicPublisher], supervisor, cmd, true)
      val ref = system.actorOf(publisher)

      Source.fromPublisher(ActorPublisher[SonicMessage](ref))
        .mapMaterializedValue { _ ⇒
          new Cancellable {
            private var cancelled = false

            override def isCancelled: Boolean = cancelled

            override def cancel(): Boolean = {
              if (!cancelled) {
                ref ! CancelStream
                cancelled = true
                cancelled
              } else false
            }
          }
        }
    }


    final def run(cmd: SonicCommand)(implicit system: ActorSystem, mat: ActorMaterializer): Future[Vector[SonicMessage]] =
      stream(cmd).toMat(fold)(Keep.right).run()


    final def authenticate(user: String, apiKey: String, traceId: String = UUID.randomUUID().toString)
                          (implicit system: ActorSystem, mat: ActorMaterializer): Future[Token] = {

      import system.dispatcher

      stream(Authenticate(user, apiKey, Some(traceId)))
        .toMat(fold)(Keep.right)
        .run()
        .flatMap(_.collectFirst {
          case OutputChunk(JsArray(Vector(JsString(token)))) ⇒ Future.successful(token)
        }.getOrElse {
          Future.failed(
            new SonicPublisher.StreamException(traceId, new Exception("protocol error: no token found in stream")))
        })
    }
  }
}
