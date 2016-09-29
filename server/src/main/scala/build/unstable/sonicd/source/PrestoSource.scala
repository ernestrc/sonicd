package build.unstable.sonicd.source

import akka.actor._
import akka.http.scaladsl.model.HttpHeader.ParsingResult
import akka.http.scaladsl.model._
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.stream.actor.ActorPublisher
import build.unstable.sonic.JsonProtocol._
import build.unstable.sonic._
import build.unstable.sonicd.source.http.HttpSupervisor
import build.unstable.sonicd.source.http.HttpSupervisor.HttpRequestCommand
import build.unstable.sonicd.{BuildInfo, SonicdConfig, SonicdLogging}
import build.unstable.tylog.Variation
import org.slf4j.event.Level
import spray.json._

import scala.collection.immutable.Seq
import scala.collection.mutable
import scala.concurrent.duration.{Duration, _}

class PrestoSource(query: Query, actorContext: ActorContext, context: RequestContext)
  extends DataSource(query, actorContext, context) {

  def prestoSupervisorProps(masterUrl: String, masterPort: Int): Props =
    Props(classOf[PrestoSupervisor], masterUrl, masterPort)

  val masterUrl: String = getConfig[String]("url")
  val masterPort: Int = getOption[Int]("port").getOrElse(8889)

  val supervisorName = Presto.getSupervisorName(masterUrl)

  def getSupervisor(name: String): ActorRef = {
    actorContext.child(name).getOrElse {
      actorContext.actorOf(prestoSupervisorProps(masterUrl, masterPort), supervisorName)
    }
  }

  lazy val publisher: Props = {
    //if no presto supervisor has been initialized yet for this presto cluster, initialize one
    val prestoSupervisor = actorContext.child(supervisorName).getOrElse {
      actorContext.actorOf(prestoSupervisorProps(masterUrl, masterPort), supervisorName)
    }

    Props(classOf[PrestoPublisher], query.traceId.get, query.query, prestoSupervisor,
      SonicdConfig.PRESTO_WATERMARK, SonicdConfig.PRESTO_MAX_RETRIES,
      SonicdConfig.PRESTO_RETRYIN, SonicdConfig.PRESTO_RETRY_MULTIPLIER,
      SonicdConfig.PRESTO_RETRY_ERRORS, context)
  }
}

class PrestoSupervisor(val masterUrl: String, val port: Int)
  extends HttpSupervisor[Presto.QueryResults] {

  lazy val debug: Boolean = false

  lazy val poolSettings: ConnectionPoolSettings = ConnectionPoolSettings(SonicdConfig.PRESTO_CONNECTION_POOL_SETTINGS)

  implicit lazy val jsonFormat: RootJsonFormat[Presto.QueryResults] = Presto.queryResultsJsonFormat

  lazy val httpEntityTimeout: FiniteDuration = SonicdConfig.PRESTO_HTTP_ENTITY_TIMEOUT

  lazy val extraHeaders: Seq[HttpHeader] = scala.collection.immutable.Seq(Presto.Headers.SOURCE)

  override def cancelRequestFromResult(t: Presto.QueryResults): Option[HttpRequest] =
    t.partialCancelUri.map { uri ⇒
      HttpRequest(HttpMethods.DELETE, uri)
    }
}

class PrestoPublisher(traceId: String, query: String,
                      supervisor: ActorRef,
                      watermark: Int,
                      maxRetries: Int,
                      retryIn: FiniteDuration,
                      retryMultiplier: Int,
                      retryErrors: Either[List[Long], Unit])
                     (implicit ctx: RequestContext)
  extends ActorPublisher[SonicMessage] with SonicdLogging {

  import Presto._
  import akka.stream.actor.ActorPublisherMessage._
  import context.dispatcher

  //in case this publisher never gets subscribed to
  override def subscriptionTimeout: Duration = 1.minute

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.debug("stopping presto publisher of '{}'", traceId)
    retryScheduled.map(c ⇒ if (!c.isCancelled) c.cancel())
    context unwatch supervisor
  }

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    log.debug("starting presto publisher of '{}'", traceId)
    context watch supervisor
  }


  /* HELPERS */

  def tryPushDownstream() {
    while (isActive && totalDemand > 0 && buffer.nonEmpty) {
      onNext(buffer.dequeue())
    }
  }

  def tryPullUpstream() {
    if (lastQueryResults.isDefined && lastQueryResults.get.nextUri.isDefined && (buffer.isEmpty || shouldQueryAhead)) {
      val cmd = HttpRequestCommand(traceId,
        HttpRequest(HttpMethods.GET, lastQueryResults.get.nextUri.get, headers = headers))
      supervisor ! cmd
      lastQueryResults = None
    }
  }

  def shouldQueryAhead: Boolean = watermark > 0 && buffer.length < watermark

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
        log.warning("could not map type {}", anyElse)
        (name, JsString(""))
    })
  }

  val submitUri = s"/${SonicdConfig.PRESTO_APIV}/statement"
  val headers: scala.collection.immutable.Seq[HttpHeader] =
    Seq(ctx.user.map(u ⇒ Headers.USER(u.user)).getOrElse(Headers.USER("sonicd")))

  val queryCommand = {
    val entity: RequestEntity = query
    val httpRequest = HttpRequest.apply(HttpMethods.POST, submitUri, entity = entity, headers = headers)
    HttpRequestCommand(traceId, httpRequest)
  }

  val units = Some("splits")


  /* STATE */

  var bufferedMeta: Boolean = false
  val buffer: mutable.Queue[SonicMessage] = mutable.Queue(StreamStarted(ctx.traceId))
  var lastQueryResults: Option[QueryResults] = None
  var retryScheduled: Option[Cancellable] = None
  var retried = 0
  var callType: CallType = ExecuteStatement
  var completedSplits = 0


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

  def materialized: Receive = commonReceive orElse {

    case Request(n) ⇒
      tryPushDownstream()
      tryPullUpstream()

    case r: QueryResults ⇒
      log.debug("received query results of query '{}'", r.id)
      lastQueryResults = Some(r)
      //extract type metadata
      if (!bufferedMeta && r.columns.isDefined) {
        buffer.enqueue(getTypeMetadata(r.columns.get))
        bufferedMeta = true
      }

      val splits = r.stats.completedSplits - completedSplits
      val totalSplits = Some(r.stats.totalSplits.toDouble)
      completedSplits = r.stats.completedSplits

      r.stats.state match {

        case "QUEUED" | "PLANNING" ⇒
          buffer.enqueue(QueryProgress(QueryProgress.Waiting, splits, totalSplits, units))
          r.data.foreach(d ⇒ d.foreach(va ⇒ buffer.enqueue(OutputChunk(va))))
          tryPullUpstream()

        case "RUNNING" | "STARTING" | "FINISHING" ⇒
          buffer.enqueue(QueryProgress(QueryProgress.Running, splits, totalSplits, units))
          r.data.foreach(d ⇒ d.foreach(va ⇒ buffer.enqueue(OutputChunk(va))))
          tryPullUpstream()

        case "FINISHED" ⇒
          r.data.foreach(d ⇒ d.foreach(va ⇒ buffer.enqueue(OutputChunk(va))))
          log.tylog(Level.INFO, traceId, callType, Variation.Success, r.stats.state)
          context.become(terminating(done = StreamCompleted.success))

        case "FAILED" ⇒
          val error = r.error.get
          val e = new Exception(error.message)
          log.tylog(Level.INFO, traceId, callType, Variation.Failure(e),
            "query status is FAILED: {}", error)

          //retry
          if ((retryErrors.isLeft && retryErrors.left.get.contains(error.errorCode) ||
            retryErrors.isRight && (error.isInternal || error.isExternal)) && retried < maxRetries) {
            retried += 1
            callType = RetryStatement(retried)
            retryScheduled = Some(context.system.scheduler
              .scheduleOnce(if (retryMultiplier > 0) retryIn * retryMultiplier * retried else retryIn,
                new Runnable {
                  override def run(): Unit = runStatement(callType, queryCommand, context.self)
                }))

          } else {
            log.debug("error_code: {}; error_type: {}; skipping retry", error.errorCode, error.errorType)
            context.become(terminating(StreamCompleted.error(e)))
          }

        case state ⇒
          val msg = s"unexpected query state from presto $state"
          val e = new Exception(msg)
          log.tylog(Level.INFO, traceId, callType, Variation.Failure(e), msg)
          context.become(terminating(StreamCompleted.error(e)))
      }
      tryPushDownstream()

    case Status.Failure(e) ⇒
      log.tylog(Level.INFO, traceId, callType, Variation.Failure(e), "something went wrong with the http request")
      context.become(terminating(StreamCompleted.error(e)))
  }

  def runStatement(callType: CallType, post: HttpRequestCommand, sender: ActorRef) {
    log.tylog(Level.INFO, traceId, callType, Variation.Attempt,
      "send query to supervisor in path {}", supervisor.path)
    supervisor.tell(post, sender)
  }

  def commonReceive: Receive = {
    case Cancel ⇒
      log.debug("client canceled")
      onComplete()
      context.stop(self)
  }

  def receive: Receive = commonReceive orElse {

    case SubscriptionTimeoutExceeded ⇒
      log.info(s"no subscriber in within subs timeout $subscriptionTimeout")
      onCompleteThenStop()

    //first time client requests
    case Request(n) ⇒
      buffer.enqueue(QueryProgress(QueryProgress.Started, 0, None, None))
      runStatement(callType, queryCommand, context.self)
      tryPushDownstream()
      context.become(materialized)
  }
}

object Presto {
  def getSupervisorName(masterUrl: String): String = s"presto_$masterUrl"

  object Headers {
    private def mkHeader(name: String, value: String): HttpHeader =
      HttpHeader.parse(name, value).asInstanceOf[ParsingResult.Ok].header

    val USER = mkHeader("X-Presto-User", _: String)
    val SOURCE = mkHeader("X-Presto-Source", "sonicd/" + BuildInfo.version + "/" + BuildInfo.commit)
    val CATALOG = mkHeader("X-Presto-Catalog", _: String)
    val SCHEMA = mkHeader("X-Presto-Schema", _: String)
    val TZONE = mkHeader("X-Presto-Time-Zone", _: String)
    val LANG = mkHeader("X-Presto-Language", _: String)
    val SESSION = mkHeader("X-Presto-Session", _: String)
    val SETSESSION = mkHeader("X-Presto-Set-Session", _: String)
    val CLEARSESSION = mkHeader("X-Presto-Clear-Session", _: String)
    val TID = mkHeader("X-Presto-Transaction-Id", _: String)
    val STARTEDTID = mkHeader("X-Presto-Started-Transaction-Id", _: String)
    val CLEARTID = mkHeader("X-Presto-Clear-Transaction-Id", _: String)
    val STATE = mkHeader("X-Presto-Current-State", _: String)
    val MAXW = mkHeader("X-Presto-Max-Wait", _: String)
    val MAXS = mkHeader("X-Presto-Max-Size", _: String)
    val TAID = mkHeader("X-Presto-Task-Instance-Id", _: String)
    val PID = mkHeader("X-Presto-Page-Sequence-Id", _: String)
    val PAGENEXT = mkHeader("X-Presto-Page-End-Sequence-Id", _: String)
    val BUFCOMPLETE = mkHeader("X-Presto-Buffer-Complete", _: String)
  }

  case class StatementStats(state: String,
                            scheduled: Boolean,
                            nodes: Int,
                            totalSplits: Int,
                            queuedSplits: Int,
                            runningSplits: Int,
                            completedSplits: Int,
                            userTimeMillis: Int,
                            cpuTimeMillis: Int,
                            processedRows: Int,
                            processedBytes: Int)

  case class ErrorLocation(lineNumber: Int, columnNumber: Int)

  case class FailureInfo(message: String,
                         stack: Vector[String],
                         errorLocation: Option[ErrorLocation])

  case class ErrorMessage(message: String,
                          errorCode: Int,
                          errorName: String,
                          errorType: String,
                          failureInfo: FailureInfo) {
    def isInternal: Boolean = errorType == "INTERNAL_ERROR"

    def isExternal: Boolean = errorType == "EXTERNAL"
  }

  case class QueryStats(elapsedTime: String,
                        queuedTime: String,
                        totalTasks: Int,
                        runningTasks: Int,
                        completedTasks: Int)

  case class Data(rows: Option[Vector[Vector[JsValue]]])

  case class Root(source: Data, columns: Vector[String])

  case class Plan(root: Root, distribution: String)

  case class OutputStage(state: String,
                         plan: Plan,
                         types: Vector[String])

  case class ColMeta(name: String, _type: String)

  case class QueryResults(id: String,
                          infoUri: String,
                          partialCancelUri: Option[String],
                          nextUri: Option[String],
                          columns: Option[Vector[ColMeta]],
                          data: Option[Vector[Vector[JsValue]]],
                          stats: StatementStats,
                          error: Option[ErrorMessage],
                          updateType: Option[String],
                          updateCount: Option[Long]) extends HttpSupervisor.Traceable {
    override def setTraceId(newId: String): HttpSupervisor.Traceable =
      this.copy(id = newId)
  }

  case class ErrorCode(code: Int, name: String)

  val USER_ERROR = "USER_ERROR"
  val SYNTAX_ERROR = "SYNTAX_ERROR"

  class PrestoError(msg: String) extends Exception(msg)

  implicit var statementStatsJsonFormat: RootJsonFormat[StatementStats] = jsonFormat11(StatementStats.apply)
  implicit var queryStatsJsonFormat: RootJsonFormat[QueryStats] = jsonFormat5(QueryStats.apply)
  implicit var colMetaJsonFormat: RootJsonFormat[ColMeta] = new RootJsonFormat[ColMeta] {
    override def write(obj: ColMeta): JsValue = throw new DeserializationException("json write of ColMeta not implemented")

    override def read(json: JsValue): ColMeta = {
      val f = json.asJsObject.fields
      ColMeta(f("name").convertTo[String], f("type").convertTo[String])
    }
  }
  implicit var dataJsonFormat: RootJsonFormat[Data] = jsonFormat1(Data.apply)
  implicit var rootJsonFormat: RootJsonFormat[Root] = jsonFormat2(Root.apply)
  implicit var planJsonFormat: RootJsonFormat[Plan] = jsonFormat2(Plan.apply)
  implicit var outputStageJsonFormat: RootJsonFormat[OutputStage] = jsonFormat3(OutputStage.apply)
  implicit var errorLocation: RootJsonFormat[ErrorLocation] = jsonFormat2(ErrorLocation.apply)
  implicit var failureInfoFormat: RootJsonFormat[FailureInfo] = jsonFormat3(FailureInfo.apply)
  implicit var errorMessageJsonFormat: RootJsonFormat[ErrorMessage] = jsonFormat5(ErrorMessage.apply)
  implicit val queryResultsJsonFormat: RootJsonFormat[QueryResults] = jsonFormat10(QueryResults.apply)

}
