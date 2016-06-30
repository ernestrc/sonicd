package build.unstable.sonicd.source

import akka.actor.Actor.Receive
import akka.actor.{Actor, ActorLogging, Props, ActorContext}
import build.unstable.sonicd.model.{SonicdLogging, DataSource, RequestContext, Query}
import build.unstable.sonicd.source.http.HttpSupervisor

object ElasticSearch {
  def getSupervisorName(masterUrl: String): String = s"elasticsearch_$masterUrl"

  case class Query
}

class ElasticSearchSource(query: Query, actorContext: ActorContext, context: RequestContext)
  extends DataSource(query, actorContext, context) {

  def elasticsearchSupervisorProps(masterUrl: String, masterPort: Int): Props =
    Props(classOf[ElasticSearchSupervisor], masterUrl, masterPort)

  val nodeUrl: String = getConfig[String]("url")
  val nodePort: Int = getConfig[Int]("port")

  val supervisorName = ElasticSearch.getSupervisorName(nodeUrl)

  lazy val handlerProps: Props = {
    //if no elasticsearch supervisor has been initialized yet for this elasticsearch cluster, initialize one
    val elasticsearchSupervisor = actorContext.child(supervisorName).getOrElse {
      actorContext.actorOf(elasticsearchSupervisorProps(nodeUrl, nodePort), supervisorName)
    }

    Props(classOf[ElasticSearchPublisher], query.traceId.get, query.query,
      elasticsearchSupervisor,
      masterUrl, SonicdConfig.PRESTO_MAX_RETRIES,
      SonicdConfig.PRESTO_RETRYIN, context)
  }
}

class ElasticSearchSupervisor(val masterUrl: String, val port: Int) extends HttpSupervisor[ElasticSearch.Query] {

  import context.dispatcher

  lazy val poolSettings: ConnectionPoolSettings = ConnectionPoolSettings(SonicdConfig.PRESTO_CONNECTION_POOL_SETTINGS)

  implicit lazy val jsonFormat: RootJsonFormat[ElasticSearch.QueryResults] = ElasticSearch.queryResultsJsonFormat

  lazy val httpEntityTimeout: FiniteDuration = SonicdConfig.PRESTO_HTTP_ENTITY_TIMEOUT

  lazy val extraHeaders: Seq[HttpHeader] = scala.collection.immutable.Seq(ElasticSearch.Headers.USER("sonicd"), ElasticSearch.Headers.SOURCE)

  override def cancelRequestFromResult(t: ElasticSearch.QueryResults): Option[HttpRequest] =
    t.partialCancelUri.map { uri ⇒
      HttpRequest(HttpMethods.DELETE, uri)
    }

  override def receive: Actor.Receive = {
    val recv: Receive = {
      case ElasticSearch.GetQueryResults(queryId, nextUri) ⇒
        val req = HttpRequest(HttpMethods.GET, nextUri)
        doRequest(queryId, req)(to[ElasticSearch.QueryResults])
          .map(r ⇒ HttpCommandSuccess(r.copy(queryId))).pipeTo(self)(sender())
    }
    recv orElse super.receive
  }
}

class ElasticSearchPublisher(traceId: String, query: String,
                      supervisor: ActorRef, masterUrl: String,
                      maxRetries: Int, retryIn: FiniteDuration,
                      ctx: RequestContext)
  extends ActorPublisher[SonicMessage] with SonicdLogging {

  import ElasticSearch._
  import akka.stream.actor.ActorPublisherMessage._
  import context.dispatcher

  //in case this publisher never gets subscribed to
  override def subscriptionTimeout: Duration = 1.minute

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    info(log, "stopping elasticsearch publisher of '{}' pointing at '{}'", traceId, masterUrl)
    retryScheduled.map(c ⇒ if (!c.isCancelled) c.cancel())
  }

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    debug(log, "starting elasticsearch publisher of '{}' pointing at '{}'", traceId, masterUrl)
  }


  /* HELPERS */

  def tryPushDownstream() {
    while (isActive && totalDemand > 0 && buffer.nonEmpty) {
      onNext(buffer.dequeue())
    }
  }

  def tryPullUpstream() {
    if (lastQueryResults.isDefined && lastQueryResults.get.nextUri.isDefined && (buffer.isEmpty || shouldQueryAhead)) {
      supervisor ! GetQueryResults(lastQueryResults.get.id, lastQueryResults.get.nextUri.get)
      lastQueryResults = None
    }
  }

  def shouldQueryAhead: Boolean = buffer.length < 2466 * 2

  def getTypeMetadata(v: Vector[ColMeta]): TypeMetadata = {
    TypeMetadata(v.map {
      case ColMeta(name, "boolean") ⇒ (name, JsBoolean(false))
      case ColMeta(name, "bigint") ⇒ (name, JsNumber(0L))
      case ColMeta(name, "double") ⇒ (name, JsNumber(0d))
      case ColMeta(name, "varchar" | "time" | "date" | "timestamp") ⇒ (name, JsString(""))
      case ColMeta(name, "varbinary") ⇒ (name, JsArray(JsNumber(0)))
      case ColMeta(name, "array") ⇒ (name, JsArray.empty)
      case ColMeta(name, "json" | "map") ⇒ (name, JsObject(Map.empty[String, JsValue]))
      case ColMeta(name, anyElse) ⇒
        warning(log, "could not map type {}", anyElse)
        (name, JsString(""))
    })
  }

  val uri = s"/${SonicdConfig.PRESTO_APIV}/statement"

  val queryRequest = {
    val entity: RequestEntity = query
    val httpRequest = HttpRequest.apply(HttpMethods.POST, uri, entity = entity)
    HttpRequestCommand(traceId, httpRequest)
  }


  /* STATE */

  var bufferedMeta: Boolean = false
  val buffer = scala.collection.mutable.Queue.empty[SonicMessage]
  var lastQueryResults: Option[QueryResults] = None
  var retryScheduled: Option[Cancellable] = None
  var retried = 0
  var callType: CallType = ExecuteStatement


  /* BEHAVIOUR */

  def terminating(done: DoneWithQueryExecution): Receive = {
    tryPushDownstream()
    if (buffer.isEmpty && isActive && totalDemand > 0) {
      onNext(done)
      onCompleteThenStop()
    }

    {
      case r: Request ⇒ terminating(done)
    }
  }

  def connected: Receive = commonReceive orElse {

    case Request(n) ⇒
      tryPushDownstream()
      tryPullUpstream()

    case r: QueryResults ⇒
      log.debug("recv query results of query '{}'", r.id)
      lastQueryResults = Some(r)
      //extract type metadata
      if (!bufferedMeta && r.columns.isDefined) {
        buffer.enqueue(getTypeMetadata(r.columns.get))
        bufferedMeta = true
      }

      r.stats.state match {
        case "RUNNING" | "QUEUED" | "PLANNING" | "STARTING" ⇒
          r.data.foreach(d ⇒ d.foreach(va ⇒ buffer.enqueue(OutputChunk(va))))
          tryPullUpstream()

        case "FINISHED" ⇒
          r.data.foreach(d ⇒ d.foreach(va ⇒ buffer.enqueue(OutputChunk(va))))
          trace(log, traceId, callType, Variation.Success, r.stats.state)
          context.become(terminating(done = DoneWithQueryExecution.success))

        case "FAILED" ⇒
          val error = r.error.get
          val e = new Exception(error.message)
          trace(log, traceId, callType, Variation.Failure(e),
            "query status is FAILED: {}", r.error.get)
          error.errorCode match {
            case 65540 /* PAGE_TRANSPORT_TIMEOUT */ if retried < maxRetries ⇒
              retried += 1
              callType = RetryStatement(retried)
              retryScheduled = Some(context.system.scheduler
                .scheduleOnce(retryIn, supervisor,
                  runStatement(callType, queryRequest)))
            case _ ⇒ context.become(terminating(DoneWithQueryExecution.error(e)))
          }

        case state ⇒
          val msg = s"unexpected query state from elasticsearch $state"
          val e = new Exception(msg)
          trace(log, traceId, callType, Variation.Failure(e), msg)
          context.become(terminating(DoneWithQueryExecution.error(e)))
      }
      tryPushDownstream()

    case Status.Failure(e) ⇒
      trace(log, traceId, callType, Variation.Failure(e), "something went wrong with the http request")
      context.become(terminating(DoneWithQueryExecution.error(e)))
  }

  def runStatement(callType: CallType, post: HttpRequestCommand) = Future {
    trace(log, traceId, callType, Variation.Attempt,
      "send query to supervisor in path {}", supervisor.path)
    supervisor ! post
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
      runStatement(callType, queryRequest)
      context.become(connected)
  }
}
