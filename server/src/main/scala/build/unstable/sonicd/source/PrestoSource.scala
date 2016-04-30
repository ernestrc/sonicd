package build.unstable.sonicd.source

import akka.actor._
import akka.http.scaladsl.model.HttpHeader.ParsingResult
import akka.http.scaladsl.model._
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.pattern._
import akka.stream.actor.ActorPublisher
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import build.unstable.sonicd.model.JsonProtocol._
import build.unstable.sonicd.model._
import build.unstable.sonicd.{SonicdConfig, Sonicd}
import spray.json._

import scala.annotation.tailrec
import scala.concurrent.Future
import scala.concurrent.duration.{Duration, _}
import scala.util.{Failure, Success}

class PrestoSource(config: JsObject, queryId: String, query: String, context: ActorContext)
  extends DataSource(config, queryId, query, context) {

  def prestoSupervisorProps(masterUrl: String, masterPort: Int): Props =
    Props(classOf[PrestoSupervisor], masterUrl, masterPort)

  val masterUrl: String = getConfig[String]("url")
  val masterPort: Int = getOption[Int]("port").getOrElse(8889)

  val supervisorName = Presto.getSupervisorName(masterUrl)

  lazy val handlerProps: Props = {
    //if no presto supervisor has been initialized yet for this presto cluster, initialize one
    val prestoSupervisor = context.child(supervisorName).getOrElse {
      context.actorOf(prestoSupervisorProps(masterUrl, masterPort), supervisorName)
    }

    Props(classOf[PrestoPublisher], queryId, query, prestoSupervisor, masterUrl)
  }
}

object Presto {
  def getSupervisorName(masterUrl: String): String = s"suppresto_$masterUrl"

  object Headers {
    private def mkHeader(name: String, value: String): HttpHeader =
      HttpHeader.parse(name, value).asInstanceOf[ParsingResult.Ok].header

    val USER = mkHeader("X-Presto-User", _: String)
    val SOURCE = mkHeader("X-Presto-Source", _: String)
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
                         errorLocation: ErrorLocation)

  case class ErrorMessage(message: String,
                          errorCode: Int,
                          errorName: String,
                          errorType: String,
                          failureInfo: FailureInfo,
                          errorLocation: ErrorLocation)

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

  case class InfoUriResults(queryId: String,
                            state: String,
                            query: String,
                            fieldNames: Vector[String],
                            queryStats: QueryStats,
                            uri: Option[String],
                            outputStage: OutputStage)

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
                          updateCount: Option[Long])

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
  implicit var infoUriResultsJsonFormat: RootJsonFormat[InfoUriResults] = jsonFormat7(InfoUriResults.apply)
  implicit var errorLocation: RootJsonFormat[ErrorLocation] = jsonFormat2(ErrorLocation.apply)
  implicit var failureInfoFormat: RootJsonFormat[FailureInfo] = jsonFormat3(FailureInfo.apply)
  implicit var errorMessageJsonFormat: RootJsonFormat[ErrorMessage] = jsonFormat6(ErrorMessage.apply)
  implicit val queryResultsJsonFormat: RootJsonFormat[QueryResults] = jsonFormat10(QueryResults.apply)

  case class GetInfo(queryId: String, infoUri: String)

  case class GetQueryResults(queryId: String, nextUri: String)

}

class PrestoSupervisor(masterUrl: String, port: Int) extends Actor with ActorLogging {

  import Presto._
  import context.dispatcher

  implicit val materializer: ActorMaterializer = ActorMaterializer(ActorMaterializerSettings(context.system))

  lazy val connectionPool = Sonicd.http.newHostConnectionPool[String](
    host = masterUrl,
    port = port,
    settings = ConnectionPoolSettings(SonicdConfig.PRESTO_CONNECTION_POOL_SETTINGS)
  )

  def doRequest[T: RootJsonFormat](queryId: String, request: HttpRequest): Future[T] = {
    Source.single(request.copy(headers = scala.collection.immutable.Seq(Headers.USER("sonicd"))) → queryId)
      .via(connectionPool)
      .runWith(Sink.head)
      .flatMap {
        case (Success(response), _) if response.status.isSuccess() =>
          log.debug("http req query for '{}' is successful", queryId)
          response.entity.toStrict(SonicdConfig.PRESTO_TIMEOUT).map { d ⇒
            val str = d.data.decodeString("UTF-8")
            log.debug("recv response from presto master for '{}': {}", queryId, str)
            str.parseJson.convertTo[T]
          }
        case (Success(response), _) ⇒
          log.debug("http request for query '{}' failed", queryId)
          val parsed = response.entity.toStrict(10.seconds)
          parsed.recoverWith {
            case e: Exception ⇒
              val error = new PrestoError(s"request failed with status ${response.status}")
              log.error(error, s"unsuccessful response from server")
              Future.failed(error)
          }
          parsed.flatMap { en ⇒
            en.toStrict(SonicdConfig.PRESTO_TIMEOUT).flatMap { entity ⇒
              val entityMsg = entity.data.decodeString("UTF-8")
              val error = new PrestoError(entityMsg)
              log.error(error, "unsuccessful response from server")
              Future.failed(error)
            }
          }
        case (Failure(e), _) ⇒ Future.failed(e)
      }
  }

  def getInfo(queryId: String, infoUri: String): Future[InfoUriResults] =
    doRequest[InfoUriResults](queryId, HttpRequest.apply(HttpMethods.GET, infoUri))
      .map(i ⇒ i.copy(uri = Some(infoUri)))

  def runStatement(queryId: String, statement: String): Future[QueryResults] = {
    val uri: Uri = s"/${SonicdConfig.PRESTO_APIV}/statement"
    val entity: RequestEntity = statement
    val httpRequest: HttpRequest = HttpRequest.apply(HttpMethods.POST, uri, entity = entity)

    doRequest[QueryResults](queryId, httpRequest).map(r ⇒ r.copy(id = queryId))
  }

  def cancelQuery(queryId: String, cancelUri: String): Future[QueryResults] =
    doRequest[QueryResults](queryId, HttpRequest(HttpMethods.GET, cancelUri))

  val queries = scala.collection.mutable.Map.empty[ActorRef, QueryResults]

  override def receive: Actor.Receive = {

    case Terminated(ref) ⇒
      queries.remove(ref).flatMap { res ⇒
        res.partialCancelUri.map { cancelUri ⇒
          val queryId = res.id
          cancelQuery(queryId, cancelUri).andThen {
            case Success(r) ⇒ log.debug("successfully canceled query '{}'", queryId)
            case Failure(e) ⇒ log.error(e, "error canceling query '{}'", queryId)
          }
        }
      }.getOrElse(log.warning("could not cancel/remove query of publisher {}", ref))

    case GetQueryResults(queryId, nextUri) ⇒
      log.debug("getting query results of '{}'", queryId)
      val req = HttpRequest(HttpMethods.GET, nextUri)
      doRequest[QueryResults](queryId, req).map(r ⇒ r.copy(id = queryId)).pipeTo(self)(sender())


    case GetInfo(queryId, uri) ⇒
      log.debug("getting info of query '{}'", queryId)
      //getInfo is sent only when state is finished, so no point in trying to cancel
      queries.remove(sender())
      getInfo(queryId, uri) pipeTo sender()

    case r: QueryResults ⇒
      val pub = sender()
      queries.update(pub, r)
      log.debug("processing query '{}'", r.id)
      pub ! r

    case q@Query(queryId, s, _) ⇒
      log.debug("{} supervising query '{}'", self.path, s)
      val pub = sender()
      context.watch(pub)
      runStatement(queryId.get, s).pipeTo(self)(pub)

    case anyElse ⇒ log.warning("recv unexpected msg: {}", anyElse)
  }

}

class PrestoPublisher(queryId: String, query: String, supervisor: ActorRef, masterUrl: String)
  extends ActorPublisher[SonicMessage] with ActorLogging {

  import Presto._
  import akka.stream.actor.ActorPublisherMessage._
  import context.dispatcher

  //in case this publisher never gets subscribed to
  override def subscriptionTimeout: Duration = 1.minute

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.info(s"stopping presto publisher of '$queryId' pointing at '$masterUrl'")
  }

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    log.info(s"starting presto publisher of '$queryId' pointing at '$masterUrl'")
  }

  def terminating(done: DoneWithQueryExecution): Receive = {
    if (isActive && totalDemand > 0) {
      onNext(done)
      onCompleteThenStop()
    }

    {
      case r: Request ⇒ terminating(done)
    }
  }

  def stream(): Boolean = {
    while (isActive && totalDemand > 0 && buffer.nonEmpty) {
      onNext(buffer.dequeue())
    }
    buffer.isEmpty
  }

  def getTypeMetadata(v: Vector[ColMeta]): TypeMetadata = {
    TypeMetadata(v.map {
      case ColMeta(name, "bigint") ⇒ (name, JsNumber(0L))
      case ColMeta(name, "varchar") ⇒ (name, JsString(""))
    })
  }

  var bufferedMeta: Boolean = false
  val buffer = scala.collection.mutable.Queue.empty[SonicMessage]

  def connected: Receive = commonReceive orElse {

    case Request(n) ⇒
      if (stream()) {} // TODO maybe fetch more results?

    //self scheduled
    case cmd: GetQueryResults ⇒ supervisor ! cmd

    case r: QueryResults ⇒
      r.stats.state match {
        case "RUNNING" | "QUEUED" ⇒
          context.system.scheduler.scheduleOnce(1.second, self, GetQueryResults(r.id, r.nextUri.get))

        case "FINISHED" ⇒
          supervisor ! GetInfo(r.id, r.infoUri)

        case "FAILED" ⇒
          val e = new Exception(r.error.get.message)
          context.become(terminating(DoneWithQueryExecution.error(e)))

        case state ⇒
          val e = new Exception(s"unexpected query state from presto $state")
          context.become(terminating(DoneWithQueryExecution.error(e)))
      }

      if (!bufferedMeta && r.columns.isDefined) {
        buffer.enqueue(getTypeMetadata(r.columns.get))
        bufferedMeta = true
      }
      r.data.foreach(d ⇒ d.foreach(va ⇒ buffer.enqueue(OutputChunk(va))))
      stream()

    case r: InfoUriResults ⇒
      log.debug(">>>>>>>>>>>>>>>>>>> WHAT NOW? recv info results {}", r)
    //r.outputStage.plan.root.source.rows.map { data ⇒
    //  context.become(streaming(data))
    //}.getOrElse {
    //}

    case Status.Failure(e) ⇒ context.become(terminating(DoneWithQueryExecution.error(e)))
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
      supervisor ! Query(Some(queryId), query, JsObject.empty)
      log.debug("sent query to supervisor {}", supervisor)
      context.become(connected)
  }
}
