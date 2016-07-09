package build.unstable.sonicd.service.source

import akka.actor.ActorRef
import akka.testkit.TestKitBase
import build.unstable.sonicd.model.{QueryProgress, DoneWithQueryExecution, TypeMetadata}

trait HandlerUtils {
  this: TestKitBase ⇒

  def expectQueryProgress(progress: Long,
                          status: QueryProgress.Status,
                          total: Option[Long],
                          units: Option[String]): QueryProgress = {
    val msg = expectMsgAnyClassOf(classOf[QueryProgress])

    assert(msg.status == status, s"status: ${msg.status} was not equal to $status")
    assert(msg.units == units, s"units ${msg.units} was not equal to $units")
    assert(msg.total == total, s"total ${msg.total} not equal to $total")
    assert(msg.progress == progress, s"progress: ${msg.progress} was not equal to $progress")
    msg
  }

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
