package build.unstable.sonic

import java.net.InetSocketAddress
import java.nio.ByteBuffer

import akka.actor.{Actor, ActorContext, ActorRef, ActorSystem, Props, Terminated}
import akka.io.{IO, Tcp}
import akka.stream.actor.ActorPublisher
import akka.stream.scaladsl.{Flow, Source}
import akka.stream.{ActorMaterializer, scaladsl}
import akka.util.{ByteString, Timeout}
import build.unstable.sonic.SonicPublisher.Ack
import build.unstable.sonicd.model.JsonProtocol._
import build.unstable.sonicd.model._
import build.unstable.tylog.Variation
import spray.json.JsObject

import scala.annotation.tailrec
import scala.concurrent.Future

case object SonicSource {

  private[sonic] def sonicSupervisorProps(addr: InetSocketAddress): Props =
    Props(classOf[SonicSupervisor], addr)

  private[sonic] def getSupervisorName(addr: InetSocketAddress): String = s"sonicd_${addr.hashCode()}"

  //length prefix framing
  def lengthPrefixEncode(bytes: ByteString): ByteString = {
    val len = ByteBuffer.allocate(4)
    len.putInt(bytes.length)
    ByteString(len.array() ++ bytes)
  }

  /**
    * Authenticates a user and returns a token. Future will fail if api-key is invalid
    */
  def authenticate(user: String, apiKey: String, address: InetSocketAddress)
                  (implicit system: ActorSystem, mat: ActorMaterializer): Future[String] = ???

  def authenticate(user: String, apiKey: String, connection: Flow[ByteString, ByteString, Future[scaladsl.Tcp.OutgoingConnection]])
                  (implicit system: ActorSystem, mat: ActorMaterializer): Future[String] = {
    ???
  }

  /* API */

  /**
    * runs the given query against the sonicd instance
    * in address
    *
    * @param address sonicd instance address
    * @param query   query to run
    */
  def run(query: Query, address: InetSocketAddress)
         (implicit system: ActorSystem, timeout: Timeout, ctx: RequestContext): Future[Vector[SonicMessage]] = {
    ???
    /*
    val prom = Promise[Vector[SonicMessage]]()

    val subscriber = new Subscriber[SonicMessage] {

      private val buffer = ListBuffer.empty[SonicMessage]

      override def onError(t: Throwable): Unit = ???

      override def onSubscribe(s: Subscription): Unit = ???

      override def onComplete(): Unit = {
      }

      override def onNext(t: SonicMessage): Unit = buffer.append(t)

    }*/

  }

  /**
    * builds [[akka.stream.scaladsl.Source]] of SonicMessage.
    * The materialized value corresponds to the number of messages
    * that were streamed and signals when the stream
    * is completed if it's a finite stream.
    *
    * @param address sonicd instance address
    * @param q       query to run
    */
  //TODO overload methods with different combination of implicits
  //TODO materialized value should be tuple with future of traceId as well
  final def stream(address: InetSocketAddress, q: Query)
                  (implicit system: ActorSystem, ctx: RequestContext, timeout: Timeout): Source[SonicMessage, Future[DoneWithQueryExecution]] = {
    import akka.pattern.ask
    val query = q.copy(trace_id = Some(ctx.traceId))
    val publisher: Props = {
      // FIXME
      val supervisorName = SonicSource.getSupervisorName(address)
      val sonicSupervisor = system.actorOf(SonicSource.sonicSupervisorProps(address), supervisorName)

      Props(classOf[SonicPublisher], sonicSupervisor, query, ctx)
    }

    val ref = system.actorOf(publisher)

    Source.fromPublisher(ActorPublisher[SonicMessage](ref))
      .mapMaterializedValue(_ ⇒ ref.ask(SonicPublisher.RegisterDone)(timeout).mapTo[DoneWithQueryExecution])
  }
}

class SonicSource(q: Query, actorContext: ActorContext, context: RequestContext)
  extends DataSource(q, actorContext, context) {

  import SonicSource._

  val host: String = getConfig[String]("host")
  val port: Int = getOption[Int]("port").getOrElse(8889)
  val config: JsObject = getConfig[JsObject]("config")
  val addr = new InetSocketAddress(host, port)

  val query: SonicCommand = Query(q.query, config, q.auth).copy(trace_id = q.traceId)

  val supervisorName = SonicSource.getSupervisorName(addr)

  def getSupervisor(name: String)(implicit system: ActorSystem): ActorRef = {
    actorContext.child(name).getOrElse {
      actorContext.actorOf(sonicSupervisorProps(addr), supervisorName)
    }
  }

  lazy val handlerProps: Props = {
    val sonicSupervisor = actorContext.child(supervisorName).getOrElse {
      actorContext.actorOf(sonicSupervisorProps(addr), supervisorName)
    }

    Props(classOf[SonicPublisher], sonicSupervisor, query, context)
  }
}

object SonicSupervisor {

  case class NewPublisher(ctx: RequestContext)

}

class SonicSupervisor(addr: InetSocketAddress) extends Actor with SonicdLogging {

  import SonicSupervisor._
  import akka.io.Tcp._

  val tcpIoService: ActorRef = IO(Tcp)(context.system)

  override def receive: Receive = {
    case NewPublisher(ctx) ⇒
      // always create new connection
      // until sonicd knows how to pipeline requests
      // diverge Connected/Failed reply to publisher
      trace(log, ctx.traceId, CreateTcpConnection, Variation.Attempt, "creating new tcp connection")
      tcpIoService.tell(Connect(addr, pullMode = true), sender())
  }
}

class SonicPublisher(supervisor: ActorRef, cmd: SonicCommand)(implicit ctx: RequestContext)
  extends ActorPublisher[SonicMessage] with SonicdLogging {

  import akka.io.Tcp._
  import akka.stream.actor.ActorPublisherMessage._

  import scala.concurrent.duration._

  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    debug(log, "stopped sonic publisher of '{}'", ctx.traceId)
    context unwatch supervisor
    if (firstSent && !firstReceived) {
      val msg = "server never sent any messages"
      val e = new Exception(msg)
      trace(log, ctx.traceId, EstablishCommunication, Variation.Failure(e), msg)
    }
    registerDone.foreach(_ !
      done.getOrElse(DoneWithQueryExecution.error(new Exception("publisher stopped unexpectedly"))))
    super.postStop()
  }

  @scala.throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    debug(log, "starting sonic publisher of '{}'", ctx.traceId)
    context watch supervisor
    supervisor ! SonicSupervisor.NewPublisher(ctx)
  }

  //in case this publisher never gets subscribed to
  override def subscriptionTimeout: Duration = 1.minute

  /* STATE */

  val buffer = scala.collection.mutable.Queue[SonicMessage](StreamStarted(ctx.traceId))
  var connection: ActorRef = _
  var pendingRead: ByteString = ByteString.empty
  var done: Option[DoneWithQueryExecution] = None
  var registerDone: Option[ActorRef] = None
  var firstSent: Boolean = false
  var firstReceived: Boolean = false


  /* HELPERS */

  def tryPushDownstream() {
    while (isActive && totalDemand > 0 && buffer.nonEmpty) {
      onNext(buffer.dequeue())
    }
  }


  /* BEHAVIOUR */

  def terminating(ev: DoneWithQueryExecution): Receive = {
    tryPushDownstream()
    done = Some(ev)
    if (buffer.isEmpty && isActive && totalDemand > 0) {
      onNext(ev)
      onCompleteThenStop()
    }

    {
      case r: Request ⇒ terminating(ev)
    }
  }

  def commonBehaviour: Receive = {

    case SonicPublisher.RegisterDone ⇒
      registerDone = Some(sender())

    case Terminated(_) ⇒
      context.become(terminating(DoneWithQueryExecution.error(new Exception("tcp connection terminated unexpectedly"))))

  }

  @tailrec
  final def frame(): Unit = {
    lazy val (lengthBytes, msgBytes) = pendingRead.splitAt(4)
    lazy val len = lengthBytes.asByteBuffer.getInt

    if (pendingRead.length > 4 && len <= msgBytes.length) {

      val (fullMsgBytes, rest) = msgBytes.splitAt(len)
      pendingRead = rest

      SonicMessage.fromBytes(fullMsgBytes) match {

        case d: DoneWithQueryExecution ⇒
          val msg = SonicSource.lengthPrefixEncode(ClientAcknowledge.toBytes)
          val write = Tcp.Write(msg, Ack)
          connection ! write

          context.become({
            case WritingResumed ⇒ connection ! write
            case t@Tcp.CommandFailed(_: Tcp.Write) ⇒ connection ! ResumeWriting
            case Ack ⇒
              context.become(terminating(d))
          }, discardOld = true)

        case m if buffer.isEmpty && isActive && totalDemand > 0 ⇒ onNext(m)
        case m ⇒ buffer.enqueue(m)
      }

      // log if first message
      if (!firstReceived) {
        firstReceived = true
        trace(log, ctx.traceId, EstablishCommunication, Variation.Success, "received first msg from server")
      }
      frame()
    }
  }

  def tryFrame(): Unit = {
    try frame()
    catch {
      case e: Exception ⇒
        error(log, e, "error framing incoming bytes")
        context.become(terminating(DoneWithQueryExecution.error(e)))
    }
  }

  def materialized: Receive = commonBehaviour orElse {
    case Request(_) ⇒
      tryFrame()
      tryPushDownstream()

    case Tcp.Received(data) ⇒
      pendingRead ++= data
      tryFrame()
      tryPushDownstream()
      connection ! ResumeReading

    case anyElse ⇒ warning(log, "received unexpected {} when in idle state", anyElse)

  }

  def connected: Receive = commonBehaviour orElse {
    trace(log, ctx.traceId, EstablishCommunication, Variation.Attempt, "sending first msg {}", cmd)
    connection ! Tcp.Register(self)
    val bytes = SonicSource.lengthPrefixEncode(cmd.toBytes)
    val write = Tcp.Write(bytes, Ack)
    connection ! write

    {
      case WritingResumed ⇒ connection ! write
      case t@Tcp.CommandFailed(_: Tcp.Write) ⇒ connection ! ResumeWriting
      case Ack ⇒
        firstSent = true
        connection ! ResumeReading
        tryPushDownstream()
        context.become(materialized)

      case anyElse ⇒ warning(log, "received unexpected {} when in connnected state", anyElse)
    }
  }

  // halfReady signals when one of the async events (Connected, Request)
  // that we're waiting for has been received already
  def idle(halfReady: Boolean): Receive = commonBehaviour orElse {

    case f@CommandFailed(_: Connect) ⇒
      trace(log, ctx.traceId, CreateTcpConnection,
        Variation.Failure(new Exception(f.cmd.failureMessage.toString)), "failed to obtain new tcp connection")
      context.become(terminating(DoneWithQueryExecution.error(new Exception(f.cmd.failureMessage.toString))))

    case SubscriptionTimeoutExceeded ⇒
      log.info(s"no subscriber in within subs timeout $subscriptionTimeout")
      onCompleteThenStop()

    case Connected(_, _) if halfReady ⇒
      trace(log, ctx.traceId, CreateTcpConnection, Variation.Success, "received tcp connection")
      connection = sender()
      context.become(connected)

    case Connected(_, _) ⇒
      trace(log, ctx.traceId, CreateTcpConnection, Variation.Success, "received new tcp connection")
      connection = sender()
      context.become(idle(halfReady = true))

    case Request(_) if halfReady ⇒
      context.become(connected)

    case Request(_) ⇒
      context.become(idle(halfReady = true))

    case anyElse ⇒ warning(log, "received unexpected {} when in idle state", anyElse)
  }

  override def receive: Receive = idle(halfReady = false)

}

object SonicPublisher {

  case object Ack extends Tcp.Event

  case object RegisterDone

}
