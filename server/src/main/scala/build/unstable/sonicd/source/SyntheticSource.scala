package build.unstable.sonicd.source

import akka.actor._
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import build.unstable.sonicd.model.JsonProtocol._
import build.unstable.sonicd.model.{DataSource, Query, RequestContext, SonicMessage}
import spray.json.{JsArray, JsNumber, JsString}

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.util.{Random, Try}

class SyntheticSource(query: Query, actorContext: ActorContext, context: RequestContext)
  extends DataSource(query, actorContext, context) {

  val handlerProps: Props = {
    val seed = getOption[Int]("seed").getOrElse(1000)
    val size = getOption[Int]("size")
    val progress = getOption[Int]("progress-delay").getOrElse(10)
    val indexed = getOption[Boolean]("indexed").getOrElse(false)

    Props(classOf[SyntheticPublisher], query.id.get, seed, size,
      progress, query.query, indexed, context)
  }
}

class SyntheticPublisher(queryId: Long, seed: Int, size: Option[Int], progressWait: Int,
                         query: String, indexed: Boolean, ctx: RequestContext)
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
        onNext(OutputChunk(JsArray(JsString(streamed.toString), JsNumber(rdm.nextInt()))))
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
        onNext(QueryProgress(1, Some(100), None))
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
      onNext(DoneWithQueryExecution.success)
      onCompleteThenStop()

    case Request(n) ⇒ stream(n)

    case Cancel ⇒
      log.debug("client canceled")
      onCompleteThenStop()
  }
}
