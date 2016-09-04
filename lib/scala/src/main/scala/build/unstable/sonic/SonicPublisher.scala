package build.unstable.sonic

import java.util.UUID

import akka.actor.{ActorRef, Terminated}
import akka.io.Tcp
import akka.stream.actor.ActorPublisher
import akka.util.ByteString
import build.unstable.sonic.SonicPublisher.{StreamException, Ack}
import build.unstable.tylog.Variation

import scala.annotation.tailrec
import scala.collection.mutable

object SonicPublisher {

  case object Ack extends Tcp.Event

  class StreamException(traceId: String, cause: Throwable)
    extends Exception(s"stream $traceId completed with errors", cause)

}

class SonicPublisher(supervisor: ActorRef, command: SonicCommand, isClient: Boolean)
  extends ActorPublisher[SonicMessage] with ClientLogging {

  import akka.io.Tcp._
  import akka.stream.actor.ActorPublisherMessage._

  import scala.concurrent.duration._

  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    debug(log, "stopped sonic publisher of '{}'", traceId)
    if (firstSent && !firstReceived) {
      val msg = "server never sent any messages"
      val e = new Exception(msg)
      trace(log, traceId, EstablishCommunication, Variation.Failure(e), msg)
    }
    super.postStop()
  }

  @scala.throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    debug(log, "starting sonic publisher of '{}'", traceId)
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

  val (cmd, traceId) = command.traceId.map(command → _).getOrElse(command → UUID.randomUUID().toString)


  /* STATE */

  val buffer =
    if (isClient) mutable.Queue[SonicMessage]()
    else mutable.Queue[SonicMessage](StreamStarted(traceId))
  var connection: ActorRef = _
  var pendingRead: ByteString = ByteString.empty
  var done: Option[StreamCompleted] = None
  var firstSent: Boolean = false
  var firstReceived: Boolean = false


  /* BEHAVIOUR */

  def terminating(ev: StreamCompleted): Receive = {
    tryPushDownstream()
    done = Some(ev)
    if (buffer.isEmpty && isActive && totalDemand > 0) {
      if (ev.error.isDefined) {
        onNext(ev)
        onErrorThenStop(new StreamException(traceId, ev.error.get))
      } else {
        onNext(ev)
        onCompleteThenStop()
      }
    }

    {
      case r: Request ⇒ terminating(ev)
    }
  }

  //TODO def waitingCancelAck then when received termiante
  def waitingCancelAck: Receive = {
    case Tcp.Received(data) ⇒
      pendingRead ++= data
      tryFrame()
      if (isActive && totalDemand > 0 && buffer.nonEmpty) {
        onNext(buffer.dequeue())
      }
      if (buffer.isEmpty) {

      }
      connection ! ResumeReading
  }

  def commonBehaviour: Receive = {
    case Cancel ⇒
      val write = Tcp.Write(Sonic.lengthPrefixEncode(CancelStream.toBytes), Ack)
      context.become({
        case WritingResumed ⇒ connection ! write
        case t@Tcp.CommandFailed(_: Tcp.Write) ⇒ connection ! ResumeWriting
        case Ack ⇒ context.become(waitingCancelAck)
      }, discardOld = true)

    case Terminated(_) ⇒
      context.become(terminating(StreamCompleted.error(traceId, new Exception("tcp connection terminated unexpectedly"))))

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
      context watch connection
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