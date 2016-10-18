package build.unstable.sonicd.system

import akka.actor.{ActorRef, Props}
import akka.io.{IO, Tcp}
import akka.routing.RoundRobinPool
import build.unstable.sonic.model.DataSource
import build.unstable.sonic.server.system.{TcpHandler, TcpSupervisor, WsHandler}
import build.unstable.sonicd.SonicdConfig
import build.unstable.sonicd.system.actor.{AuthenticationActor, SonicdController}

/**
 * Trait that declares the actors that make up our service
 */
trait Service {

  /** handles the underlying low level I/O resources (selectors, channels)
    * and instantiates workers for specific tasks, such as listening to incoming connections.
    */
  val tcpIoService: ActorRef

  /** listens for new connections and creates instances of [[TcpHandler]] */
  val tcpService: ActorRef

  /**
   * instantiates [[DataSource]] subclasses in
   * response to Query commands. Monitors [[TcpHandler]] and
   * [[WsHandler]]. Handles resource authorization
   */
  val controllerService: ActorRef

  /**
   * creates and validates auth tokens
   */
  val authenticationService: ActorRef
}

/**
 * This trait creates the actors that make up our application; it can be mixed in with
 * ``Service`` for running code or ``TestKit`` for unit and integration tests.
 */
trait AkkaService extends Service {
  this: System ⇒

  val authenticationService: ActorRef = system.actorOf(
    RoundRobinPool(SonicdConfig.AUTH_WORKERS)
      .props(Props(classOf[AuthenticationActor], SonicdConfig.API_KEYS,
        SonicdConfig.AUTH_SECRET, SonicdConfig.TOKEN_DURATION)), "authentication")

  val tcpIoService: ActorRef = IO(Tcp)

  val controllerService: ActorRef = system.actorOf(Props(classOf[SonicdController],
    authenticationService, SonicdConfig.ACTOR_TIMEOUT), "controller")

  val tcpService = system.actorOf(Props(classOf[TcpSupervisor], controllerService, authenticationService), "tcpSupervisor")

}
