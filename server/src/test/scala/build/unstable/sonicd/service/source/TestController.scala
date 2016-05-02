package build.unstable.sonicd.service.source

import akka.actor.{OneForOneStrategy, SupervisorStrategy, ActorRef, Actor}
import build.unstable.sonicd.model.DoneWithQueryExecution

class TestController(implicitSender: ActorRef) extends Actor {


  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(loggingEnabled = true){
    case e: Exception ⇒ implicitSender ! DoneWithQueryExecution.error(e); SupervisorStrategy.Stop
  }

  override def receive: Receive = {
    case _ ⇒
  }
}

