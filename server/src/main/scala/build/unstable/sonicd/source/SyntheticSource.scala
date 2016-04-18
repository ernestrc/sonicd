package build.unstable.sonicd.source

import akka.actor._
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import build.unstable.sonicd.model.JsonProtocol._
import build.unstable.sonicd.model.{DataSource, SonicMessage}
import spray.json.{JsNumber, JsObject}

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.util.{Random, Try}

class SyntheticSource(config: JsObject, queryId: String, query: String, context: ActorContext)
  extends DataSource(config, queryId, query, context) {

  val handlerProps: Props = {
    val seed = config.fields.get("seed").map(_.convertTo[Int]).getOrElse(1000)
    val size = config.fields.get("size").map(_.convertTo[Int])
    val progress = config.fields.get("progress-delay").map(_.convertTo[Int]).getOrElse(10)
    val indexed = config.fields.get("indexed").map(_.convertTo[Boolean]).getOrElse(false)
    Props(classOf[SyntheticPublisher], seed, size, progress, query, indexed)
  }
}

class SyntheticPublisher(seed: Int, size: Option[Int], progressWait: Int,
                         query: String, indexed: Boolean)
  extends Actor with ActorPublisher[SonicMessage] with ActorLogging {

  import build.unstable.sonicd.model._

  //in case this publisher never gets subscribed to
  override def subscriptionTimeout: Duration = 10.seconds

  val rdm = new Random(seed)

  var streamed = 0L
  val preTarget = 101 //+100 of progress +1 metadata
  val target =
    Try(query.trim().toInt)
      .recoverWith {
        case e: Exception ⇒
          log.warning("could not parse query to determine test target size")
          Try(size.get)
      }.toOption.map(_ + preTarget)

  @tailrec
  private def stream(demand: Long): Unit = {
    if (totalDemand > 0) {
      if (indexed) {
        onNext(OutputChunk(Vector(streamed, rdm.nextInt())))
      } else onNext(OutputChunk(Vector(rdm.nextInt())))
      streamed += 1
      stream(demand - 1L)
    }
  }

  @tailrec
  private def progress(demand: Long): Unit = {
    if (totalDemand > 0L && isActive) {
      if (streamed < preTarget) {
        Thread.sleep(progressWait)
        onNext(QueryProgress(Some(1), None))
        streamed += 1
        progress(demand - 1L)
      } else {
        self ! Request(demand)
      }
    }
  }

  def receive: Receive = {

    case Request(n) if streamed == 0L ⇒
      log.info(s"starting synthetic stream with target of '$target'")
      val m = TypeMetadata(Vector("data" → JsNumber(0)))
      onNext(if (indexed) m.copy(typesHint = Vector("index" → JsNumber(0)) ++ m.typesHint) else m)
      streamed += 1
      progress(n)

    case Request(n) if streamed < preTarget ⇒ progress(n)

    case Request(n) if target.nonEmpty && streamed >= target.get ⇒
      log.info(s"reached target of ${target.get - preTarget}")
      onNext(DoneWithQueryExecution(success = true))
      onCompleteThenStop()

    case Request(n) ⇒ stream(n)

    case Cancel ⇒
      log.debug("client canceled")
      onCompleteThenStop()
  }
}
