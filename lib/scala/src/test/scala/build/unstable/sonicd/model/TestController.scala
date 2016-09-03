package build.unstable.sonicd.model

import akka.actor.{Actor, ActorRef, OneForOneStrategy, SupervisorStrategy}
import build.unstable.sonic.{ClientLogging, StreamCompleted}

class TestController(implicitSender: ActorRef) extends Actor with ClientLogging {

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(loggingEnabled = true){
    case e: Exception ⇒ implicitSender ! StreamCompleted.error("test-trace-id", e); SupervisorStrategy.Stop
  }

  override def receive: Receive = {
    case anyElse ⇒ warning(log, "extraneous message recv {}", anyElse)
  }
}

