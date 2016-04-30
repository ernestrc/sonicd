package build.unstable.sonicd.system

import akka.actor.{ActorRef, Props}
import akka.http.scaladsl.Http
import akka.io.{Tcp, IO}
import akka.stream._

import scala.concurrent.duration._

/**
 * Trait that declares the actors that make up our service
 */
trait Service {

  /** handles the underlying low level I/O resources (selectors, channels)
    * and instantiates workers for specific tasks, such as listening to incoming connections.
    */
  val tcpIoService: ActorRef

  /** listens for new connections and creates instances of [[WsHandler]] */
  val tcpService: ActorRef

  /**
   * responds to api commands and monitors [[WsHandler]] and [[WsHandler]]
   */
  val controllerService: ActorRef
}

/**
 * This trait creates the actors that make up our application; it can be mixed in with
 * ``Service`` for running code or ``TestKit`` for unit and integration tests.
 */
trait AkkaService extends Service {
  this: System ⇒

  val controllerService: ActorRef = system.actorOf(Props(classOf[SonicController], materializer), "controller")

  val tcpIoService: ActorRef = IO(Tcp)

  val tcpService = system.actorOf(Props(classOf[TcpSupervisor], controllerService), "tcpSupervisor")

}
