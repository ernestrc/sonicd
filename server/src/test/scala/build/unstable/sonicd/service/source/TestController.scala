package build.unstable.sonicd.service.source

import akka.actor.{Actor, ActorRef, OneForOneStrategy, SupervisorStrategy}
import build.unstable.sonicd.model.{DoneWithQueryExecution, RequestContext, SonicdLogging}

class TestController(implicitSender: ActorRef) extends Actor with SonicdLogging {

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(loggingEnabled = true){
    case e: Exception ⇒ implicitSender ! DoneWithQueryExecution.error("test-trace-id", e); SupervisorStrategy.Stop
  }

  override def receive: Receive = {
    case anyElse ⇒ warning(log, "extraneous message recv {}", anyElse)
  }
}

