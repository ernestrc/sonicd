package build.unstable.sonicd.source

import java.util.concurrent.TimeUnit

import akka.actor._
import akka.stream.actor.ActorPublisher
import build.unstable.sonicd.model.JsonProtocol._
import build.unstable.sonicd.model._
import build.unstable.sonicd.source.HiveThriftService.Command
import build.unstable.tylog.Variation
import com.twitter.finagle.Thrift
import com.twitter.scrooge.ThriftStruct
import com.twitter.util.{Timer, Future}
import org.apache.hive.service.cli.thrift._

import scala.concurrent.duration._

class HiveSource(query: Query, actorContext: ActorContext, ctx: RequestContext)
  extends DataSource(query, actorContext, ctx) {

  val hiveServerTimeout: Long = getOption[Long]("timeout").getOrElse(10000)

  def hiveServiceProps(hiveServerUrl: String): Props =
    Props(classOf[HiveThriftService], hiveServerUrl,
      com.twitter.util.Duration(hiveServerTimeout, TimeUnit.MILLISECONDS))

  val hiveServerUrl: String = getConfig[String]("url")
  val initializationStmts: List[String] = getOption[List[String]]("pre").getOrElse(Nil)

  //if no thrift-service actor has been initialized yet, initialize one
  lazy val thriftService = actorContext.child(HiveThriftService.getHiveServiceActorName(hiveServerUrl)).getOrElse {
    actorContext.actorOf(hiveServiceProps(hiveServerUrl), HiveThriftService.getHiveServiceActorName(hiveServerUrl))
  }

  override lazy val handlerProps: Props =
    Props(classOf[HivePublisher],
      query.traceId.get, query.query, ctx, thriftService, initializationStmts)
}

class HivePublisher(traceId: String,
                    query: String,
                    ctx: RequestContext,
                    thriftService: ActorRef,
                    initializationStmts: List[String])
  extends ActorPublisher[SonicMessage] with SonicdLogging {

  import akka.stream.actor.ActorPublisherMessage._

  override def subscriptionTimeout: Duration = 1.minute

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    debug(log, "stopping hive publisher of '{}'", traceId)
    if (sesHandle != null) thriftService ! Command(TCloseSessionReq(sesHandle), ctx)
    if (opHandle != null) thriftService ! Command(TCloseOperationReq(opHandle), ctx) //TODO set back to null when op is done
  }

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    debug(log, "starting hive publisher of '{}'", traceId)
  }

  context watch thriftService

  /* HELPERS */

  def tryPushDownstream() {
    while (isActive && totalDemand > 0 && buffer.nonEmpty) {
      onNext(buffer.dequeue())
    }
  }


  /* STATE */

  var sesHandle: TSessionHandle = null
  var opHandle: TOperationHandle = null
  var bufferedMeta: Boolean = false
  val buffer = scala.collection.mutable.Queue.empty[SonicMessage]
  var callType: CallType = GetConnectionHandle


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

  def running(_opHandle: TOperationHandle): Receive = {
    opHandle = _opHandle

    {
      case t: TGetOperationStatusResp ⇒ log.info(t.toString) //TODO
    }
  }

  def connected(_sesHandle: TSessionHandle): Receive = {
    sesHandle = _sesHandle

    {
      case TExecuteStatementResp(TStatus(TStatusCode.SuccessStatus | TStatusCode.SuccessWithInfoStatus, _, _, _, _), opHandleMaybe) ⇒
        //TODO trace
        val opHandle = opHandleMaybe.get
        thriftService ! Command(TGetOperationStatusReq(opHandle), ctx)
        context.become(running(opHandleMaybe.get))

      case TExecuteStatementResp(status, _) ⇒ log.error(status.toString)
      //TODO trace
    }
  }

  def materialized: Receive = commonReceive orElse {

    case Request(_) ⇒
      tryPushDownstream()
    //TODO tryPullUpstream()

    case TOpenSessionResp(TStatus(TStatusCode.SuccessStatus | TStatusCode.SuccessWithInfoStatus, _, _, _, _), _, handleMaybe, _) ⇒
      buffer.enqueue(QueryProgress(QueryProgress.Running, 0, None, None))
      val handle = handleMaybe.get
      trace(log, traceId, callType, Variation.Success, "get connection handle success")
      callType = ExecuteStatement
      trace(log, traceId, callType, Variation.Attempt, "executing statement..")
      thriftService ! Command(TExecuteStatementReq(handle, query, runAsync = true), ctx)

      tryPushDownstream()
      context.become(connected(handle))

    case TOpenSessionResp(status, _, handleMaybe, _) ⇒
      val e = new Exception(s"unexpected TOpenSessionResp status: $status")
      trace(log, traceId, callType, Variation.Failure(e), "failed to get connection handle")
      context.become(terminating(DoneWithQueryExecution.error(e)))

    case Status.Failure(e) ⇒
      trace(log, traceId, callType, Variation.Failure(e), "something went wrong with the thrift call")
      context.become(terminating(DoneWithQueryExecution.error(e)))
  }

  def receive: Receive = commonReceive orElse {

    //first time client requests
    case Request(n) ⇒
      buffer.enqueue(QueryProgress(QueryProgress.Started, 0, None, None))
      trace(log, traceId, callType, Variation.Attempt,
        "getting connection handle from thrift service in path {}", thriftService.path)
      val user: String = ctx.user.map(_.user).getOrElse("hadoop")
      thriftService ! Command(TOpenSessionReq(username = Some(user)), ctx)
      tryPushDownstream()
      context.become(materialized)

    case SubscriptionTimeoutExceeded ⇒
      log.info(s"no subscriber in within subs timeout $subscriptionTimeout")
      onCompleteThenStop()
  }

  def commonReceive: Receive = {
    case t: Terminated ⇒
      val e = new Exception("thrift-service terminated unexpectedly")
      trace(log, traceId, callType, Variation.Failure(e), "")
      context.become(terminating(DoneWithQueryExecution.error(e)))
    case Cancel ⇒
      log.debug("client canceled")
      onComplete()
      context.stop(self)
  }
}

class HiveThriftService(hiveServerUrl: String, timeout: com.twitter.util.Duration) extends Actor with SonicdLogging {


  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    debug(log, "starting hive thrift service {}", self.path)
  }

  @throws[Exception](classOf[Exception])
  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    debug(log, "restarting hive thrift service {}, message {}: reason: {}", self.path, reason.toString, message)
    super.preRestart(reason, message)
  }

  val thrift = Thrift.client.newIface[TCLIService[com.twitter.util.Future]](hiveServerUrl)

  override def receive: Actor.Receive = {
    case Command(underlying, ctx) ⇒
      val receiver = sender()
      val traceReq = ThriftReq(underlying.getClass.getName)

      trace(log, ctx.traceId, traceReq, Variation.Attempt,
        "running thrift call for {}", receiver)

      val future: com.twitter.util.Future[_] = underlying match {
        case req: TOpenSessionReq ⇒ thrift.openSession(req)
        case req: TExecuteStatementReq ⇒ thrift.executeStatement(req)
        case req: TCloseSessionReq ⇒ thrift.closeSession(req)
        case req: TGetInfoReq ⇒ thrift.getInfo(req)
        case req: TGetTypeInfoReq => thrift.getTypeInfo(req)
        case req: TGetCatalogsReq => thrift.getCatalogs(req)
        case req: TGetSchemasReq => thrift.getSchemas(req)
        case req: TGetTablesReq => thrift.getTables(req)
        case req: TGetTableTypesReq => thrift.getTableTypes(req)
        case req: TGetColumnsReq => thrift.getColumns(req)
        case req: TGetFunctionsReq => thrift.getFunctions(req)
        case req: TGetOperationStatusReq => thrift.getOperationStatus(req)
        case req: TCancelOperationReq => thrift.cancelOperation(req)
        case req: TCloseOperationReq => thrift.closeOperation(req)
        case req: TGetResultSetMetadataReq => thrift.getResultSetMetadata(req)
        case req: TFetchResultsReq => thrift.fetchResults(req)
        case req: TGetDelegationTokenReq => thrift.getDelegationToken(req)
        case req: TCancelDelegationTokenReq => thrift.cancelDelegationToken(req)
        case req: TRenewDelegationTokenReq => thrift.renewDelegationToken(req)
      }

      future
        .onSuccess { value ⇒
          trace(log, ctx.traceId, traceReq, Variation.Success, "thrift call for {} succeeded", receiver)
          receiver ! value
        }
        .onFailure { cause ⇒
          trace(log, ctx.traceId, traceReq, Variation.Failure(cause), "thrift call for {} failed", receiver)
          receiver ! Status.Failure(cause)
        }

    case anyElse ⇒
      log.warn("requests need to be wrapped in a Command envelope")
  }
}

object HiveThriftService {

  case class Command(req: ThriftStruct, context: RequestContext)

  def getHiveServiceActorName(hiveServerUrl: String): String =
  //TODO replace only if necessary
    s"hive-thrift-${hiveServerUrl.replace("/", "")}"
}


