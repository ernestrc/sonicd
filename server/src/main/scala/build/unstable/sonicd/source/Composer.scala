package build.unstable.sonicd.source

import akka.actor._
import akka.stream.ActorMaterializer
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request, SubscriptionTimeoutExceeded}
import akka.stream.scaladsl._
import build.unstable.sonic.JsonProtocol._
import build.unstable.sonic.model._
import build.unstable.sonicd.SonicdLogging
import build.unstable.sonicd.source.Composer.{ComposeStrategy, ComposedQuery, ConcatStrategy}
import build.unstable.sonicd.system.actor.SonicdController
import build.unstable.sonicd.system.actor.SonicdController.SonicdQuery
import build.unstable.tylog.Variation
import org.slf4j.event.Level
import spray.json._

import scala.collection.mutable
import scala.util.matching.Regex

class Composer(query: Query, actorContext: ActorContext, context: RequestContext)
  extends SonicdSource(query, actorContext, context) with SonicdLogging {

  import Composer._

  val injectQueryPlaceholder = getOption[String]("inject-query-placeholder")

  implicit val combinedQueryJsonFormat: JsonFormat[ComposedQuery] =
    Composer.getComposedQueryJsonFormat(injectQueryPlaceholder, query.query, context)
  val queries = getConfig[List[ComposedQuery]]("queries")

  assert(queries.nonEmpty, "expected at least one query in `queries` property")
  assert(queries.forall(_.priority >= 0), "'priority' field in query config must be an unsigned integer")

  val bufferSize = getOption[Int]("buffer-size").getOrElse(2048)
  val strategy = getOption[ComposeStrategy]("strategy").getOrElse(MergeStrategy)
  val failFast = getOption[Boolean]("fail-fast").getOrElse(true)

  val actorMaterializer = ActorMaterializer.create(actorContext)

  val publisher: Props = {
    Props(classOf[ComposerPublisher], queries, bufferSize, strategy, failFast,
      context, actorMaterializer)
  }
}

object Composer {

  case class ComposedQuery(query: SonicdQuery, priority: Int, name: Option[String] = None)

  sealed trait ComposeStrategy

  case object MergeStrategy extends ComposeStrategy

  case object ConcatStrategy extends ComposeStrategy

  implicit val strategyJsonFormat: RootJsonFormat[ComposeStrategy] = new RootJsonFormat[ComposeStrategy] {
    override def read(json: JsValue): ComposeStrategy = json match {
      case JsString("concat") ⇒ ConcatStrategy
      case JsString("merge") ⇒ MergeStrategy
      case JsString(a) ⇒ throw new Exception(
        s"possible values for strategy are `merge` and `concat` found: $a")
      case e ⇒ throw new Exception(s"expected JsString found: $e")
    }

    override def write(obj: ComposeStrategy): JsValue = {
      obj match {
        case ConcatStrategy ⇒ JsString("concat")
        case MergeStrategy ⇒ JsString("merge")
      }
    }
  }

  def getComposedQueryJsonFormat(placeholder: Option[String], query: String, context: RequestContext) = {
    new RootJsonFormat[ComposedQuery] {
      override def read(json: JsValue): ComposedQuery = {
        val fields = json.asJsObject().fields
        val priority = fields.get("priority").flatMap(_.convertTo[Option[Int]]).getOrElse(0)
        val name = fields.get("name").flatMap(_.convertTo[Option[String]])
        val obj = json.asJsObject().fields
        val q =
          obj.getOrElse("query", throw getException("query")).convertTo[String]
        val replaced: String = placeholder.map { place ⇒
          val rgx = new Regex(place)
          rgx.replaceAllIn(q, query)
        }.getOrElse(q)
        val config: JsValue = obj.getOrElse("config", throw getException("config"))
        ComposedQuery(SonicdQuery(new Query(None, Some(context.traceId), None, replaced, config)), priority, name)
      }

      override def write(obj: ComposedQuery): JsValue = {
        JsObject(Map(
          "priority" → JsNumber(obj.priority),
          "query" → JsString(obj.query.query.query),
          "name" → obj.name.map(JsString.apply).getOrElse(JsNull),
          "config" → obj.query.query.config
        ))
      }
    }
  }

  private def getException(s: String): Exception = {
    new Exception(s"expected field `$s` but not field with that name found")
  }
}

class ComposerPublisher(queries: List[ComposedQuery], bufferSize: Int, strategy: ComposeStrategy,
                        failFast: Boolean)(implicit ctx: RequestContext, materializer: ActorMaterializer)
  extends ActorPublisher[SonicMessage] with SonicdLogging with SonicdPublisher {

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

  override def unhandled(message: Any): Unit = {
    log.warning("recv undhandled message {}", message)
  }


  /* HELPERS */

  def tryPushDownstream() {
    while (isActive && totalDemand > 0 && buffer.nonEmpty) {
      onNext(buffer.dequeue())
    }
  }

  implicit val priorityMessageOrdering =
    new Ordering[(Source[(build.unstable.sonic.model.SonicMessage, Int), akka.NotUsed], Int)] {
      override def compare(x: (Source[(build.unstable.sonic.model.SonicMessage, Int), akka.NotUsed], Int),
                           y: (Source[(build.unstable.sonic.model.SonicMessage, Int), akka.NotUsed], Int)): Int = {
        if (x._2 < y._2) 1
        else if (x._2 > y._2) -1
        else 0
      }
    }

  def updateProgress(subStreamProgress: QueryProgress): Boolean =
    subStreamProgress.status == QueryProgress.Running && {
      val prog = 1.0 * subStreamProgress.progress / streamsLeft / subStreamProgress.total.getOrElse(100d) * 100
      progress = QueryProgress(QueryProgress.Running, prog, Some(100d), Some("%"))
      true
    }


  /* STATE */

  val buffer: mutable.Queue[SonicMessage] = mutable.Queue(StreamStarted(ctx.traceId))
  val deferred = mutable.Queue.empty[(SonicMessage, Int)]
  var pendingAck: Boolean = false
  var progress: QueryProgress = _
  var streamsLeft: Int = _
  var streamsByPriority: mutable.Map[Int, Int] = _
  var allowedPriority: Int = _


  /* BEHAVIOUR */

  def commonReceive: Receive = {
    case Completed ⇒ context.become(terminating(StreamCompleted.success))
    case Status.Failure(e) ⇒ context.become(terminating(StreamCompleted.error(ctx.traceId, e)))
    case Cancel ⇒
      log.debug("client canceled")
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

  def sendAckMaybe(upstream: ActorRef) {
    if (totalDemand > 0) {
      upstream ! Ack
    } else {
      pendingAck = true
    }
  }

  def materialized(upstream: ActorRef): Receive = commonReceive orElse {
    case Request(n) ⇒
      tryPushDownstream()
      if (totalDemand > 0 && pendingAck) {
        upstream ! Ack
        pendingAck = false
      }
    case (m: SonicMessage, priority: Int) if priority >= allowedPriority ⇒
      m match {
        case c: StreamCompleted if failFast && c.error.isDefined ⇒
          context.become(terminating(StreamCompleted.error(c.error.get)))
        case c: StreamCompleted ⇒
          try {
            streamsLeft -= 1
            val left = streamsByPriority(priority)
            if (left == 1) {
              if (streamsLeft > 0) {
                streamsByPriority.remove(priority)
                log.debug("changing allowed priority: previous allowed priority {}", allowedPriority)
                allowedPriority = streamsByPriority.maxBy(_._1)._1
                deferred.dequeueAll(_._2 == allowedPriority).foreach(materialized(upstream)(_))
                log.debug("changing allowed priority: next allowed priority {}", allowedPriority)
              } else context.become(terminating(StreamCompleted.success))
            } else streamsByPriority.update(priority, left - 1)
          } catch {
            case e: Exception ⇒
              context.become(terminating(StreamCompleted.error(e)))
          } finally tryPushDownstream()
        case p: QueryProgress ⇒
          if (updateProgress(p)) {
            buffer.enqueue(progress)
            tryPushDownstream()
          }
        case t: TypeMetadata ⇒
          if (updateMeta(t)) {
            buffer.enqueue(meta.asInstanceOf[SonicMessage])
            tryPushDownstream()
          }
        case o: OutputChunk ⇒
          buffer.enqueue(m)
          tryPushDownstream()
        case _: StreamStarted ⇒ //ignore
      }
      sendAckMaybe(upstream)
    case (m: SonicMessage, p: Int) ⇒
      sendAckMaybe(upstream)
      deferred.enqueue(m → p)
      if (deferred.size > bufferSize) {
        val e = new Exception(s"reached deferred buffer limit of $bufferSize")
        context.become(terminating(StreamCompleted.error(e)))
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

  override def receive: Receive = commonReceive orElse {
    case SubscriptionTimeoutExceeded ⇒
      log.info("no subscriber in within subs timeout {}", subscriptionTimeout)
      onCompleteThenStop()

    case Request(n) ⇒
      log.tylog(Level.DEBUG, ctx.traceId, CombineSources, Variation.Attempt, "client requested first element")
      tryPushDownstream()
      try {
        val ps = queries.map { query ⇒
          val source = SonicdController.getDataSource(query.query.query, context, ctx.user)
          val ref = query.name
            .map(n ⇒ context.actorOf(source.publisher, n))
            .getOrElse(context.actorOf(source.publisher))
          Source.fromPublisher[SonicMessage](ActorPublisher.apply(ref))
            .map((_, query.priority)) → query.priority
        }.sorted

        log.debug("sorted queries by priority {}", ps)

        // assign max priority as current(ly streaming)
        val (sources, _current) = ps.unzip
        allowedPriority = _current.head
        streamsLeft = ps.length
        streamsByPriority = _current.foldLeft(mutable.Map.empty[Int, Int]) { (acc, el) ⇒
          acc.updated(el, acc.getOrElse(el, 0) + 1)
        }

        log.trace("streams sources {}", sources)
        log.trace("streams priorities {}", streamsByPriority)

        val merged = if (sources.length > 1) {
          val second = sources(1)
          val tail = sources.slice(2, streamsLeft)
          log.debug("merging {}:{}:{}", sources.head, second, tail)
          Source.combine(sources.head, second, tail: _*)(if (strategy == ConcatStrategy) Concat(_) else Merge(_))
        } else sources.head

        log.debug("combined graphs: {}", merged)
        merged.to(Sink.actorRefWithAck(self, Started, Ack, Completed)).run()
        context.become(waiting)

      } catch {
        case e: Exception ⇒
          context.become(terminating(StreamCompleted.error(e)))
          log.tylog(Level.DEBUG, ctx.traceId, CombineSources, Variation.Failure(e),
            "failed to build {} graph", strategy)
      }

      log.tylog(Level.DEBUG, ctx.traceId, CombineSources, Variation.Success,
        "successfully built combined {} graph", strategy)
  }
}
