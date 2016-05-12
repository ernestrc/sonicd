package build.unstable.sonicd.source

import akka.actor._
import akka.stream.actor.ActorPublisher
import build.unstable.sonicd.model.{DoneWithQueryExecution, SonicMessage, DataSource}
import spray.json.JsObject

import scala.concurrent.duration._

class HiveSource(config: JsObject, queryId: String, query: String, context: ActorContext)
  extends DataSource(config, queryId, query, context) {

  val hiveServerUrl: String = getConfig[String]("url")
  val initializationStmts: List[String] = getOption[List[String]]("pre").getOrElse(Nil)
  val user: String = getOption[String]("user").getOrElse("hadoop")

  override lazy val handlerProps: Props =
    Props(classOf[HivePublisher], queryId, query, hiveServerUrl, user, initializationStmts)
}

class HivePublisher(queryId: String,
                    query: String,
                    hiveServerUrl: String,
                    user: String,
                    initializationStmts: List[String])
  extends ActorPublisher[SonicMessage] with ActorLogging {

  import akka.stream.actor.ActorPublisherMessage._

  override def subscriptionTimeout: Duration = 1.minute

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.info(s"stopping jdbc publisher of '$queryId'")
  }

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    log.info(s"starting hive publisher of '$queryId' pointing at hive server '$hiveServerUrl'")
  }

  def receive: Receive = {

    case SubscriptionTimeoutExceeded ⇒
      log.info(s"no subscriber in within subs timeout $subscriptionTimeout")
      onCompleteThenStop()

    //first time client requests
    case Request(n) ⇒ //TODO
    case Cancel ⇒ onCompleteThenStop()
  }

}
