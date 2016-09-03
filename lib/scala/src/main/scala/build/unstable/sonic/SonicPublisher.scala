package build.unstable.sonic

import java.util.UUID

import akka.actor.{ActorRef, Terminated}
import akka.io.Tcp
import akka.stream.actor.ActorPublisher
import akka.util.ByteString
import build.unstable.sonic.SonicPublisher.Ack
import build.unstable.tylog.Variation

import scala.annotation.tailrec
import scala.collection.mutable

object SonicPublisher {

  case object Ack extends Tcp.Event

  case object RegisterDone

}

class SonicPublisher(supervisor: ActorRef, command: SonicCommand, isClient: Boolean)
  extends ActorPublisher[SonicMessage] with ClientLogging {

  import akka.io.Tcp._
  import akka.stream.actor.ActorPublisherMessage._

  import scala.concurrent.duration._

  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    debug(log, "stopped sonic publisher of '{}'", traceId)
    context unwatch supervisor
    if (firstSent && !firstReceived) {
      val msg = "server never sent any messages"
      val e = new Exception(msg)
      trace(log, traceId, EstablishCommunication, Variation.Failure(e), msg)
    }
    registerDone.foreach(_ !
      done.getOrElse(StreamCompleted.error(traceId, new Exception("publisher stopped unexpectedly"))))
    super.postStop()
  }

  @scala.throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    debug(log, "starting sonic publisher of '{}'", traceId)
    context watch supervisor
    supervisor ! SonicSupervisor.RegisterPublisher(traceId)
  }

  //in case this publisher never gets subscribed to
  override def subscriptionTimeout: Duration = 1.minute


  /* HELPERS */

  def tryPushDownstream() {
    while (isActive && totalDemand > 0 && buffer.nonEmpty) {
      onNext(buffer.dequeue())
    }
  }

  val (cmd, traceId) = command.traceId.map(command → _).getOrElse(command → UUID.randomUUID().toString)


  /* STATE */

  val buffer =
    if (isClient) mutable.Queue[SonicMessage]()
    else mutable.Queue[SonicMessage](StreamStarted(traceId))
  var connection: ActorRef = _
  var pendingRead: ByteString = ByteString.empty
  var done: Option[StreamCompleted] = None
  var registerDone: Option[ActorRef] = None
  var firstSent: Boolean = false
  var firstReceived: Boolean = false


  /* BEHAVIOUR */

  def terminating(ev: StreamCompleted): Receive = {
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
      context.become(terminating(StreamCompleted.error(traceId, new Exception("tcp connection terminated unexpectedly"))))

  }

  @tailrec
  final def frame(): Unit = {
    lazy val (lengthBytes, msgBytes) = pendingRead.splitAt(4)
    lazy val len = lengthBytes.asByteBuffer.getInt

    if (pendingRead.length > 4 && len <= msgBytes.length) {

      val (fullMsgBytes, rest) = msgBytes.splitAt(len)
      pendingRead = rest

      SonicMessage.fromBytes(fullMsgBytes) match {

        case d: StreamCompleted ⇒
          val msg = Sonic.lengthPrefixEncode(ClientAcknowledge.toBytes)
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
        trace(log, traceId, EstablishCommunication, Variation.Success, "received first msg from server")
      }
      frame()
    }
  }

  def tryFrame(): Unit = {
    try frame()
    catch {
      case e: Exception ⇒
        error(log, e, "error framing incoming bytes")
        context.become(terminating(StreamCompleted.error(traceId, e)))
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
    trace(log, traceId, EstablishCommunication, Variation.Attempt, "sending first msg {}", cmd)
    connection ! Tcp.Register(self)
    val bytes = Sonic.lengthPrefixEncode(cmd.toBytes)
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
      trace(log, traceId, CreateTcpConnection,
        Variation.Failure(new Exception(f.cmd.failureMessage.toString)), "failed to obtain new tcp connection")
      context.become(terminating(StreamCompleted.error(traceId, new Exception(f.cmd.failureMessage.toString))))

    case SubscriptionTimeoutExceeded ⇒
      log.info(s"no subscriber in within subs timeout $subscriptionTimeout")
      onCompleteThenStop()

    case Connected(_, _) if halfReady ⇒
      trace(log, traceId, CreateTcpConnection, Variation.Success, "received tcp connection")
      connection = sender()
      context.become(connected)

    case Connected(_, _) ⇒
      trace(log, traceId, CreateTcpConnection, Variation.Success, "received new tcp connection")
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