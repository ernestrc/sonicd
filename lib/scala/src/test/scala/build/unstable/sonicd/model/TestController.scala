package build.unstable.sonicd.model

import akka.actor.{Actor, ActorRef, OneForOneStrategy, SupervisorStrategy}
import build.unstable.sonic.DoneWithQueryExecution

class TestController(implicitSender: ActorRef) extends Actor with SonicdLogging {

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(loggingEnabled = true){
    case e: Exception ⇒ implicitSender ! DoneWithQueryExecution.error("test-trace-id", e); SupervisorStrategy.Stop
  }

  override def receive: Receive = {
    case anyElse ⇒ warning(log, "extraneous message recv {}", anyElse)
  }
}

