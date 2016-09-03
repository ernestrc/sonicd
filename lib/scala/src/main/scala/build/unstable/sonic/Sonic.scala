package build.unstable.sonic

import java.net.InetSocketAddress
import java.nio.ByteBuffer

import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.stream.actor.ActorPublisher
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.util.{ByteString, Timeout}

import scala.concurrent.Future

case object Sonic {

  private[unstable] def sonicSupervisorProps(addr: InetSocketAddress): Props =
    Props(classOf[SonicSupervisor], addr)

  //length prefix framing
  private[unstable] def lengthPrefixEncode(bytes: ByteString): ByteString = {
    val len = ByteBuffer.allocate(4)
    len.putInt(bytes.length)
    ByteString(len.array() ++ bytes)
  }

  case class Client(address: InetSocketAddress)(implicit system: ActorSystem) {

    val supervisor = system.actorOf(Sonic.sonicSupervisorProps(address))

    type Token = String

    /**
     * builds a [[akka.stream.scaladsl.Source]] of SonicMessage(s).
     * The materialized value signals, if the stream is finite, when the stream is completed
     */
    final def stream(query: Query)
                    (implicit system: ActorSystem, timeout: Timeout): Source[SonicMessage, Future[StreamCompleted]] = {
      val publisher: Props = Props(classOf[SonicPublisher], supervisor, query, true)
      val ref = system.actorOf(publisher)

      Source.fromPublisher(ActorPublisher[SonicMessage](ref))
        .mapMaterializedValue(_ ⇒ ref.ask(SonicPublisher.RegisterDone)(timeout).mapTo[StreamCompleted])
    }

    final def run(query: Query)
                 (implicit system: ActorSystem, timeout: Timeout, mat: ActorMaterializer): Future[Vector[SonicMessage]] = {
      stream(query)
        .toMat(Sink.fold[Vector[SonicMessage], SonicMessage](Vector.empty[SonicMessage])((a, e) ⇒ a :+ e))(Keep.right)
        .run()
    }

    /**
     * Authenticates a user and returns a token. Future will fail if api-key is invalid
     */
    final def authenticate(user: String, apiKey: String)
                          (implicit system: ActorSystem, mat: ActorMaterializer): Future[Token] = {
      ???
    }
  }
}
