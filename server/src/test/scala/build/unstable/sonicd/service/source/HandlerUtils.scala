package build.unstable.sonicd.service.source

import akka.actor.ActorRef
import akka.testkit.TestKitBase
import build.unstable.sonicd.model.{DoneWithQueryExecution, TypeMetadata}

trait HandlerUtils {
  this: TestKitBase ⇒


  def expectTypeMetadata(): TypeMetadata = {
    expectMsgAnyClassOf(classOf[TypeMetadata])
  }

  def expectDone(pub: ActorRef, success: Boolean = true): DoneWithQueryExecution = {
    val d = expectMsgType[DoneWithQueryExecution]
    if (success) assert(d.success)
    else assert(!d.success)

    expectMsg("complete") //sent by ImplicitSubscriber
    expectTerminated(pub)
    d
  }
}
