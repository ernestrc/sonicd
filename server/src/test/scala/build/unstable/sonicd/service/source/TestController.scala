package build.unstable.sonicd.service.source

import akka.actor.{OneForOneStrategy, SupervisorStrategy, ActorRef, Actor}
import build.unstable.sonicd.model.{SonicdLogging, DoneWithQueryExecution}

class TestController(implicitSender: ActorRef) extends Actor with SonicdLogging {


  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(loggingEnabled = true){
    case e: Exception ⇒ implicitSender ! DoneWithQueryExecution.error(e); SupervisorStrategy.Stop
  }

  override def receive: Receive = {
    case anyElse ⇒ warning(log, "extraneous message recv {}", anyElse)
  }
}

