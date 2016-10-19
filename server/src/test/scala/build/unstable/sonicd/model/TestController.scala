package build.unstable.sonicd.model

import akka.actor.{Actor, ActorRef, OneForOneStrategy, SupervisorStrategy}
import build.unstable.sonic.client.ClientLogging
import build.unstable.sonic.model.StreamCompleted

class TestController(implicitSender: ActorRef) extends Actor with ClientLogging {

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(loggingEnabled = true){
    case e: Exception ⇒ implicitSender ! StreamCompleted.error("test-trace-id", e); SupervisorStrategy.Stop
  }

  override def receive: Receive = {
    case anyElse ⇒ log.warning( "extraneous message recv {}", anyElse)
  }
}

