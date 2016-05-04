package build.unstable.sonicd.service

import akka.actor._
import akka.testkit.{TestKitBase, TestActorRef, ImplicitSender}

/**
 * To catch child exceptions
 */
trait ImplicitGuardian {
  this: ImplicitSender with TestKitBase ⇒

  val guardian: TestActorRef[MockGuardian] =
    TestActorRef[MockGuardian](Props(classOf[MockGuardian], self))

}

class MockGuardian(implicitSender: ActorRef) extends Actor {

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(loggingEnabled = true) {
    case t: Throwable ⇒ implicitSender ! t; SupervisorStrategy.Stop
  }

  override def receive: Receive = {
    case _ ⇒
  }
}
