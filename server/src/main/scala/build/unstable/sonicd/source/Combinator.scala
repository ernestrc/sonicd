package build.unstable.sonicd.source

import akka.actor._
import akka.stream.ActorMaterializer
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request, SubscriptionTimeoutExceeded}
import akka.stream.scaladsl._
import build.unstable.sonic.model._
import build.unstable.sonicd.SonicdLogging
import build.unstable.sonicd.source.Combinator.{CombineStrategy, ConcatStrategy, CombinedQuery}
import build.unstable.sonicd.system.actor.SonicdController
import build.unstable.sonicd.system.actor.SonicdController.SonicdQuery
import spray.json._

import scala.collection.mutable

class Combinator(query: Query, actorContext: ActorContext, context: RequestContext)
  extends SonicdSource(query, actorContext, context) {

  import Combinator._
  import build.unstable.sonic.JsonProtocol._

  implicit val sonicdQueryJsonFormat: JsonFormat[SonicdQuery] = new RootJsonFormat[SonicdQuery] {
    override def read(json: JsValue): SonicdQuery = {
      val obj = json.asJsObject().fields
      val query = obj.getOrElse("query", throw getException("query")).convertTo[String]
      val config: JsValue = obj.getOrElse("config", throw getException("config"))
      SonicdQuery(new Query(None, Some(context.traceId), None, query, config))
    }

    override def write(obj: SonicdQuery): JsValue = ???
  }

  implicit val combinedQueryJsonFormat: JsonFormat[CombinedQuery] =
    new RootJsonFormat[CombinedQuery] {
      override def read(json: JsValue): CombinedQuery = {
        val fields = json.asJsObject().fields
        val priority = fields.get("priority").map(_.convertTo[Int]).getOrElse(0)
        CombinedQuery(json.convertTo[SonicdQuery], priority)
      }

      override def write(obj: CombinedQuery): JsValue = ???
    }

  val queries = getConfig[List[CombinedQuery]]("queries")

  assert(queries.nonEmpty, "expected at least one query")
  assert(queries.forall(_.priority >= 0), "'priority' field in query config must be a positive integer")

  // TODO inject query into queries val inject = getOption[String]("placeholder")
  val bufferSize = getOption[Int]("buffer").getOrElse(256)
  val strategy = getOption[CombineStrategy]("strategy").getOrElse(MergeStrategy)
  val interleave = getOption[Boolean]("interleave").getOrElse(true)

  // TODO
  assert(interleave, "non-interleaved with priority is not implemented yet")

  val actorMaterializer = ActorMaterializer.create(actorContext)

  val publisher: Props = {
    Props(classOf[CombinatorActor], queries, bufferSize, strategy, interleave,
      context, actorMaterializer)
  }
}

object Combinator {

  case class CombinedQuery(query: SonicdQuery, priority: Int)

  sealed trait CombineStrategy

  case object MergeStrategy extends CombineStrategy

  case object ConcatStrategy extends CombineStrategy

  implicit val strategyJsonFormat: RootJsonFormat[CombineStrategy] = new RootJsonFormat[CombineStrategy] {
    override def read(json: JsValue): CombineStrategy = json match {
      case JsString("concat") ⇒ ConcatStrategy
      case JsString("merge") ⇒ MergeStrategy
      case JsString(a) ⇒ throw new Exception(
        s"possible values for strategy are `merge` and `concat` found: $a")
      case e ⇒ throw new Exception(s"expected JsString found: $e")
    }

    override def write(obj: CombineStrategy): JsValue = ???
  }

  private def getException(s: String): Exception = {
    new Exception(s"expected field `$s` but not field with that name found")
  }
}

class CombinatorActor(queries: List[CombinedQuery], bufferSize: Int,
                      strategy: CombineStrategy, interleave: Boolean)
                     (implicit ctx: RequestContext, materializer: ActorMaterializer)
  extends ActorPublisher[SonicMessage] with SonicdLogging {

  case object Ack

  case object Started

  case object Completed

  /* OVERRIDES */

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.debug("stopping combinator publisher of '{}'", ctx.traceId)
  }

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    log.debug("starting combinator publisher of '{}'", ctx.traceId)
  }


  /* HELPERS */

  def tryPushDownstream() {
    while (isActive && totalDemand > 0 && buffer.nonEmpty) {
      onNext(buffer.dequeue())
    }
  }

  def saveSendersPriority(sender: ActorRef, priority: Int): Unit = {
    publishers.get(sender) match {
      case None ⇒
        publishersN += 1
        publishers.update(sender, priority)
      case _ ⇒
    }
  }


  /* STATE */

  // TODO implement incremental meta
  var bufferedMeta: Boolean = false
  val buffer: mutable.Queue[SonicMessage] = mutable.Queue(StreamStarted(ctx.traceId))
  var pendingAck: Boolean = false
  val deferredBuffer: mutable.Queue[SonicMessage] = mutable.Queue.empty
  val publishers: mutable.Map[ActorRef, Int] = mutable.Map.empty[ActorRef, Int]
  var publishersN = 0
  var streams: Int = _
  var current: Int = _


  /* BEHAVIOUR */

  def commonReceive: Receive = {
    case Completed ⇒ // TODO ???
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


  def materialized(upstream: ActorRef): Receive = commonReceive orElse {
    case Request(n) ⇒
      tryPushDownstream()
      if (totalDemand > 0 && pendingAck) {
        upstream ! Ack
        pendingAck = false
      }

    case (m: SonicMessage, priority: Int) ⇒
      try {
        if (streams > publishersN) saveSendersPriority(sender(), priority)

        if (interleave || priority >= current) {
          buffer.enqueue(m)
        } else {
          deferredBuffer.enqueue(m)
        }

        tryPushDownstream()
      } finally {
        if (totalDemand > 0) {
          upstream ! Ack
        } else {
          pendingAck = true
        }
      }
  }

  def waiting: Receive = commonReceive orElse {
    case Request(n) ⇒ tryPushDownstream()
    case Started ⇒
      tryPushDownstream()
      log.debug("materialized combined sources for {}", ctx.traceId)

      sender() ! Ack
      context.become(materialized(sender()))
  }

  override def receive: Receive = {
    case SubscriptionTimeoutExceeded ⇒
      log.info(s"no subscriber in within subs timeout $subscriptionTimeout")
      onCompleteThenStop()

    case Request(n) ⇒
      tryPushDownstream()

      try {
        val ps = queries.map { query ⇒
          val source = SonicdController.getDataSource(query.query.query, context, ctx.user)
          val ref = context.actorOf(source.publisher)
          Source.fromPublisher[SonicMessage](ActorPublisher.apply(ref))
            .map((_, query.priority)) → query.priority
        }.sortBy(_._2)

        // assign max priority as current(ly streaming)
        current = ps.head._2
        val sources = ps.map(_._1)

        val merged: Source[(SonicMessage, Int), _] = sources.tail match {
          case tailHead :: tailsTail ⇒
            if (strategy == ConcatStrategy)
              Source.combine(sources.head, tailHead, tailsTail: _*)(Concat(_))
            else
              Source.combine(sources.head, tailHead, tailsTail: _*)(Merge(_))
          case Nil ⇒ sources.head
        }

        merged.to(Sink.actorRefWithAck(self, Started, Ack, Completed)).run()

        streams = sources.length

        context.become(waiting)
      } catch {
        case e: Exception ⇒
          context.become(terminating(StreamCompleted.error(e)))
      }
  }
}