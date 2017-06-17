package build.unstable.sonicd.source

import akka.actor._
import akka.event.Logging.LogLevel
import akka.http.scaladsl.model._
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.stream.actor.ActorPublisher
import akka.util.ByteString
import build.unstable.sonic.JsonProtocol._
import build.unstable.sonic.model._
import build.unstable.sonicd.source.ElasticSearch.ESQuery
import build.unstable.sonicd.source.http.HttpSupervisor
import build.unstable.sonicd.source.http.HttpSupervisor.{HttpRequestCommand, Traceable}
import build.unstable.sonicd.{SonicdConfig, SonicdLogging}
import build.unstable.tylog.Variation
import org.slf4j.event.Level
import spray.json._

import scala.collection.mutable
import scala.concurrent.duration.{Duration, FiniteDuration, _}
import scala.util.Try

object ElasticSearch {
  def getSupervisorName(nodeUrl: String, port: Int): String = s"elasticsearch_${nodeUrl}_$port"

  case class ESQuery(extractedFrom: Option[Long], extractedSize: Option[Long], payload: JsObject)

  case object ESQueryJsonFormat {
    //returns index (default _all), _type (default null), and parsed es query
    def read(json: JsValue): (String, Option[String], ESQuery) = {
      val obj = json.asJsObject
      val fields = obj.fields
      val index = fields.get("_index").flatMap(_.convertTo[Option[String]]).getOrElse("_all")
      val typeHint = fields.get("_type").flatMap(_.convertTo[Option[String]])

      (index, typeHint,
        ESQuery(fields.get("from").flatMap(_.convertTo[Option[Long]]),
          fields.get("size").flatMap(_.convertTo[Option[Long]]), obj))
    }

    def write(obj: ESQuery, from: Long, size: Long): JsValue = {
      val fields = mutable.Map.empty ++ obj.payload.fields

      //if not configured it will reject query
      fields.remove("_type")
      fields.remove("_index")
      fields.update("from", JsNumber(from))
      fields.update("size", JsNumber(size))

      JsObject(fields.toMap)
    }
  }

  case class Shards(total: Int, successful: Int, failed: Int)

  case class Hit(_index: String, _type: String, _id: String, _score: Float, _source: Option[JsObject])

  case class Hits(total: Long, max_score: Float, hits: Vector[Hit])

  case class QueryResults(traceId: Option[String], took: Long, timed_out: Boolean,
                          _shards: Shards, hits: Hits)
    extends HttpSupervisor.Traceable {
    def id = traceId.get

    def setTraceId(newId: String): Traceable = this.copy(traceId = Some(newId))
  }

  implicit val shardsFormat: RootJsonFormat[Shards] = jsonFormat3(Shards.apply)
  implicit val hitFormat: RootJsonFormat[Hit] = jsonFormat5(Hit.apply)
  implicit val hitsFormat: RootJsonFormat[Hits] = jsonFormat3(Hits.apply)
  implicit val queryResultsFormat: RootJsonFormat[QueryResults] = jsonFormat5(QueryResults.apply)
}

class ElasticSearchSource(query: Query, actorContext: ActorContext, context: RequestContext)
  extends SonicdSource(query, actorContext, context) {

  def elasticsearchSupervisorProps(nodeUrl: String, masterPort: Int): Props =
    Props(classOf[ElasticSearchSupervisor], nodeUrl, masterPort)

  val nodeUrl: String = getConfig[String]("url")
  val nodePort: Int = getConfig[Int]("port")

  val (index, typeHint, esQuery) = ElasticSearch.ESQueryJsonFormat.read(query.query.parseJson)

  val supervisorName = ElasticSearch.getSupervisorName(nodeUrl, nodePort)

  def getSupervisor(name: String): ActorRef = {
    actorContext.child(name).getOrElse {
      actorContext.actorOf(elasticsearchSupervisorProps(nodeUrl, nodePort), supervisorName)
    }
  }

  lazy val publisher: Props = {
    //if no ES supervisor has been initialized yet for this ES cluster, initialize one
    val supervisor = getSupervisor(supervisorName)

    Props(classOf[ElasticSearchPublisher], query.traceId.get, esQuery,
      index, typeHint, SonicdConfig.ES_QUERY_SIZE, supervisor, SonicdConfig.ES_WATERMARK, context)
  }
}

class ElasticSearchSupervisor(val masterUrl: String, val port: Int) extends HttpSupervisor[ElasticSearch.QueryResults] {

  lazy val jsonFormat: RootJsonFormat[ElasticSearch.QueryResults] = ElasticSearch.queryResultsFormat

  lazy val poolSettings: ConnectionPoolSettings = ConnectionPoolSettings(SonicdConfig.ES_CONNECTION_POOL_SETTINGS)

  lazy val httpEntityTimeout: FiniteDuration = SonicdConfig.ES_HTTP_ENTITY_TIMEOUT

  lazy val extraHeaders = scala.collection.immutable.Seq.empty[HttpHeader]

  lazy val debug: Boolean = false

  override def cancelRequestFromResult(t: ElasticSearch.QueryResults): Option[HttpRequest] = None
}

class ElasticSearchPublisher(traceId: String,
                             query: ESQuery,
                             index: String,
                             typeHint: Option[String],
                             querySize: Long,
                             supervisor: ActorRef,
                             watermark: Long)
                            (implicit ctx: RequestContext)
  extends ActorPublisher[SonicMessage] with SonicdPublisher with SonicdLogging {

  import akka.stream.actor.ActorPublisherMessage._

  override def subscriptionTimeout: Duration = 1.minute

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.info("stopping ES publisher {}", traceId)
    context unwatch supervisor
  }

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    log.debug("starting ES publisher {}", traceId)
    context watch supervisor
  }


  /* HELPERS */

  def nextRequest: HttpRequestCommand = {
    val payload: String = ElasticSearch.ESQueryJsonFormat.write(query, nextFrom, nextSize).compactPrint
    log.trace("sending query: {}", payload)
    val entity: RequestEntity =
      HttpEntity.Strict.apply(ContentTypes.`application/json`, ByteString(payload, ByteString.UTF_8))
    val httpRequest = HttpRequest.apply(HttpMethods.POST, uri, entity = entity)
    HttpRequestCommand(traceId, httpRequest)
  }

  def tryPushDownstream() {
    while (isActive && totalDemand > 0 && buffer.nonEmpty) {
      onNext(buffer.dequeue())
    }
  }

  def tryPullUpstream() {
    if (target > 0 && fetched + nextSize > target) nextSize = target - fetched
    if (!resultsPending && (buffer.isEmpty || shouldQueryAhead)) {
      resultsPending = true
      supervisor ! nextRequest
    }
  }

  def shouldQueryAhead: Boolean = watermark > 0 && buffer.length < watermark

  def getTypeMetadata(hit: ElasticSearch.Hit): TypeMetadata = {
    TypeMetadata(hit._source.map(_.fields.toVector).getOrElse(Vector.empty))
  }

  val uri = typeHint.map(t ⇒ s"/$index/$t/_search").getOrElse(s"/$index/_search")
  val limit = query.extractedSize.getOrElse(-1L)


  /* STATE */

  var target = limit
  val buffer: mutable.Queue[SonicMessage] = mutable.Queue(StreamStarted(ctx.traceId))
  var nextSize = if (limit > 0) Math.min(limit, querySize) else querySize
  var nextFrom = query.extractedFrom.getOrElse(0L)
  var fetched = 0L
  var resultsPending = false


  /* BEHAVIOUR */

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

  def getSelect(query: ESQuery): Vector[String] = {
    val payload = query.payload.fields
    payload.get("stored_fields")
      .orElse(payload.get("fields"))
      .orElse(payload.get("_source"))
      .flatMap(f ⇒ Try(f.convertTo[Vector[String]]).toOption)
      .getOrElse(Vector.empty)
  }

  def materialized: Receive = commonReceive orElse {

    case Request(n) ⇒
      tryPushDownstream()
      tryPullUpstream()

    case r: ElasticSearch.QueryResults ⇒
      val nhits = r.hits.hits.size

      resultsPending = false
      fetched += nhits
      if (target < 0L) target = r.hits.total

      // emit artificial TypeMetadata to conform to sonic protocol
      if (r.hits.total == 0 || r.hits.hits.forall(_._source.isEmpty)) {
        val select = getSelect(query)
        // no need to use updateMeta as if hits is 0 we only
        // get here once and complete stream
        meta = TypeMetadata(select.map(i ⇒ i → JsNull))
        buffer.enqueue(meta)
      }

      r.hits.hits.foreach { h ⇒
        val extracted = getTypeMetadata(h)
        if (updateMeta(extracted)) {
          buffer.enqueue(meta)
        }
        // ignore if hit doesn't contain _source
        h._source.foreach(f ⇒ buffer.enqueue(alignOutput(f.fields, meta)))
      }

      if (nhits < nextSize || fetched == target) {
        log.tylog(Level.INFO, traceId, ExecuteStatement, Variation.Success, "fetched {} documents", fetched)
        context.become(terminating(StreamCompleted.success))
      } else {
        nextFrom += nhits
        tryPullUpstream()
      }
      tryPushDownstream()

    case Status.Failure(e) ⇒
      log.tylog(Level.INFO, traceId, ExecuteStatement, Variation.Failure(e), "something went wrong with the http request")
      context.become(terminating(StreamCompleted.error(e)))
  }

  def commonReceive: Receive = {
    case Cancel ⇒
      log.debug("client canceled")
      onCompleteThenStop()
  }

  def receive: Receive = commonReceive orElse {

    case SubscriptionTimeoutExceeded ⇒
      log.info(s"no subscriber in within subs timeout $subscriptionTimeout")
      onCompleteThenStop()

    //first time client requests
    case Request(n) ⇒
      log.tylog(Level.INFO, traceId, ExecuteStatement, Variation.Attempt,
        "send query to supervisor in path {}", supervisor.path)
      tryPullUpstream()
      tryPushDownstream()
      context.become(materialized)
  }
}
