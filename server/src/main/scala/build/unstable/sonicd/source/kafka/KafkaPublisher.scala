package build.unstable.sonicd.source.kafka

import akka.actor.{ActorRef, Status}
import akka.kafka.ConsumerSettings
import akka.stream._
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request, SubscriptionTimeoutExceeded}
import akka.stream.scaladsl.{Sink, Source}
import build.unstable.sonic.JsonProtocol._
import build.unstable.sonic.model._
import build.unstable.sonicd.SonicdLogging
import build.unstable.sonicd.source.json.JsonUtils
import build.unstable.sonicd.source.json.JsonUtils.ParsedQuery
import org.apache.kafka.clients.consumer.ConsumerRecord
import spray.json._

import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.Try
import scala.util.control.NonFatal

class KafkaPublisher[K, V](supervisor: ActorRef, query: Query, settings: ConsumerSettings[K, V],
                           kFormat: JsonFormat[K], vFormat: JsonFormat[V], ignoreParsingErrors: Option[Int])
                          (implicit ctx: RequestContext, materializer: ActorMaterializer)
  extends ActorPublisher[SonicMessage] with SonicdLogging {

  // messages used with actorRefWithAck method to signal backpressure/completion with
  // underlying kafka consumer
  case object Ack

  case object Started

  case object Completed

  //in case this publisher never gets subscribed to
  override def subscriptionTimeout: Duration = 1.minute

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.debug("stopping kafka publisher of '{}'", ctx.traceId)
  }

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    log.debug("starting kafka publisher of '{}'", ctx.traceId)
  }

  override def unhandled(message: Any): Unit = {
    log.warning("{}({}) unexpected message: {}", self.path, this.getClass, message)
    super.unhandled(message)
  }

  /* HELPERS */

  def tryPushDownstream() {
    while (isActive && totalDemand > 0 && buffer.nonEmpty) {
      onNext(buffer.dequeue())
    }
  }

  // TODO should return select as well so that we can filter on that
  // like LocalJsonSource
  def parseQuery(query: Query): (String, Option[Int], Option[Long], ParsedQuery) = {
    val raw = query.query
    val obj = raw.parseJson.asJsObject(s"query must be a valid JSON object: $raw").fields
    val parsed = JsonUtils.parseQuery(obj)

    val topic: String = {
      val value = Try(obj("topic")).getOrElse(throw new Exception("missing 'topic' in query"))

      Try(value.convertTo[String])
        .getOrElse(throw new Exception("'partition' in query must be an integer"))
    }

    val partition: Option[Int] = obj.get("partition").map(p ⇒ Try(p.convertTo[Int])
      .getOrElse(throw new Exception("'partition' in query must be an integer")))

    val offset: Option[Long] = obj.get("offset").map(p ⇒ Try(p.convertTo[Long])
      .getOrElse(throw new Exception("'offset' in query must be an integer")))

    (topic, partition, offset, parsed)
  }

  // TODO implement incremental type metadata
  def parseRecord(c: ConsumerRecord[K, V]): Option[SonicMessage] = ??? /*Try {
    if (c.key() != null) Map("key" → c.key.toJson(kFormat), "value" → c.value.toJson(vFormat))
    else Map("value" → c.value().toJson(vFormat))
  }.recover {
    case NonFatal(e) if ignoreParsingErrors.isDefined && errors < ignoreParsingErrors.get ⇒
      errors += 1
      Map.empty
    case NonFatal(e) ⇒ throw e
  }.get*/

  /* STATE */

  var bufferedMeta: Boolean = false
  var pendingAck: Boolean = false
  var errors: Int = 0
  val buffer: mutable.Queue[SonicMessage] = mutable.Queue(StreamStarted(ctx.traceId))
  var callType: CallType = ExecuteStatement
  val (topic, partition, offset, filter) = parseQuery(query)

  /* BEHAVIOUR */

  def commonReceive: Receive = {
    case Status.Failure(e) ⇒ context.become(terminating(StreamCompleted.error(ctx.traceId, e)))
    case s: StreamCompleted ⇒ context.become(terminating(s))
    case Completed ⇒ context.become(terminating(StreamCompleted.success(ctx.traceId)))
    case Cancel ⇒
      log.debug("client canceled")
      onComplete()
      context.stop(self)
  }

  def terminating(done: StreamCompleted): Receive = {
    tryPushDownstream()
    if (buffer.isEmpty && isActive && totalDemand > 0) {
      onNext(done)
      onCompleteThenStop()
    }

    {
      case r: Request ⇒ terminating(done)
    }
  }

  def materialized(upstream: ActorRef): Receive =
    commonReceive orElse {
      case Request(n) ⇒
        tryPushDownstream()
        if (totalDemand > 0 && pendingAck) {
          upstream ! Ack
          pendingAck = false
        }

      case c: ConsumerRecord[_, _] ⇒
        log.trace("Received record {}", c)
        val record = c.asInstanceOf[ConsumerRecord[K, V]]

        /* TODO
        if (filter(record)) {
          val message = parseRecord(record)
          if (totalDemand > 0) {
            onNext(message)
          } else {
            buffer.enqueue(message)
          }
        } else log.trace("filtered out record {}", record)

        if (totalDemand > 0) {
          upstream ! Ack
        } else {
          pendingAck = true
        }*/
    }

  def waiting: Receive = commonReceive orElse {
    case Request(n) ⇒ tryPushDownstream()
    case Started ⇒
      buffer.enqueue(QueryProgress(QueryProgress.Started, 0, None, None))
      tryPushDownstream()
      log.debug("materialized kafka stream for {}", ctx.traceId)

      sender() ! Ack
      context.become(materialized(sender()))

    case s: Source[_, _] ⇒
      log.debug("received kafka stream source {} for {}", s, ctx.traceId)
      s.asInstanceOf[Source[ConsumerRecord[K, V], _]].to(Sink.actorRefWithAck(self, Started, Ack, Completed)).run()
  }

  override def receive: Receive = commonReceive orElse {
    case SubscriptionTimeoutExceeded ⇒
      log.info(s"no subscriber in within subs timeout $subscriptionTimeout")
      onCompleteThenStop()

    //first time client requests
    case Request(n) ⇒
      buffer.enqueue(QueryProgress(QueryProgress.Waiting, 0, None, None))
      supervisor ! KafkaSupervisor.GetBroadcastHub(topic, partition, offset, settings)
      tryPushDownstream()
      context.become(waiting)
  }
}
