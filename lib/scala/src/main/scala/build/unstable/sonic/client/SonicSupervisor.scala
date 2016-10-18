package build.unstable.sonic.client

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorRef}
import akka.io.{IO, Tcp}
import build.unstable.tylog.Variation
import org.slf4j.event.Level

// new publishers need to register themselves with an instance of this actor
// which will pair them with a tcp connection actor
class SonicSupervisor(addr: InetSocketAddress) extends Actor with ClientLogging {

  import SonicSupervisor._
  import akka.io.Tcp._

  val tcpIoService: ActorRef = IO(Tcp)(context.system)

  override def receive: Receive = {
    case RegisterPublisher(traceId) ⇒
      // always create new connection
      // until sonicd knows how to pipeline requests
      // diverge Connected/Failed reply to publisher
      log.tylog(Level.DEBUG, traceId, CreateTcpConnection, Variation.Attempt, "creating new tcp connection")
      tcpIoService.tell(Connect(addr, pullMode = true), sender())
  }
}

object SonicSupervisor {

  case class RegisterPublisher(traceId: String)

}