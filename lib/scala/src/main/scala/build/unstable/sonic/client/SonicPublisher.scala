package build.unstable.sonic.client

import java.util.UUID

import akka.actor.{ActorRef, Terminated}
import akka.io.Tcp
import akka.stream.actor.{ActorPublisher, ActorPublisherMessage}
import akka.util.ByteString
import build.unstable.sonic.client.SonicPublisher.{Ack, StreamException}
import build.unstable.sonic.model._
import build.unstable.tylog.Variation
import org.slf4j.event.Level

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
    log.debug("stopped sonic publisher of '{}'", traceId)
    if (firstSent && !firstReceived) {
      val msg = "server never sent any messages"
      val e = new Exception(msg)
      log.tylog(Level.DEBUG, traceId, EstablishCommunication, Variation.Failure(e), msg)
    }
    super.postStop()
  }

  @scala.throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    log.debug("starting sonic publisher of '{}'", traceId)
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
            case Ack ⇒ context.become(terminating(d))
          }, discardOld = true)

        case m if buffer.isEmpty && isActive && totalDemand > 0 ⇒ onNext(m)
        case m ⇒ buffer.enqueue(m)
      }

      // log if first message
      if (!firstReceived) {
        firstReceived = true
        log.tylog(Level.DEBUG, traceId, EstablishCommunication, Variation.Success, "received first msg from server")
      }
      frame()
    }
  }

  def tryFrame(): Unit = {
    try frame()
    catch {
      case e: Exception ⇒
        log.error(e, "error framing incoming bytes")
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
    log.debug("terminating with {}", ev)
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
    } else if (!isActive) {
      context.stop(self)
    }

    {
      case r: Request ⇒ terminating(ev)
    }
  }

  def commonBehaviour: Receive = {
    case ActorPublisherMessage.Cancel ⇒
      log.info("client cancelled query {}", traceId)
      val write = Tcp.Write(Sonic.lengthPrefixEncode(CancelStream.toBytes), Ack)
      connection ! write
      context.become({
        case WritingResumed ⇒ connection ! write
        case t@Tcp.CommandFailed(_: Tcp.Write) ⇒ connection ! ResumeWriting
        case Ack ⇒ context.unbecome()
      }, discardOld = false)

    case Terminated(_) ⇒
      context.become(terminating(StreamCompleted.error(traceId, new Exception("tcp connection terminated unexpectedly"))))
  }

  def materialized: Receive = commonBehaviour orElse {
    case ActorPublisherMessage.Request(_) ⇒
      tryFrame()
      tryPushDownstream()

    case Tcp.Received(data) ⇒
      pendingRead ++= data
      tryFrame()
      tryPushDownstream()
      connection ! ResumeReading

    // FIXME CancelStream recv in some scenarios
    case anyElse ⇒ log.warning("received unexpected {} when in idle state", anyElse)

  }

  def connected: Receive = commonBehaviour orElse {
    log.tylog(Level.DEBUG, traceId, EstablishCommunication, Variation.Attempt, "sending first msg {}", cmd)
    connection ! Tcp.Register(self)
    val bytes = Sonic.lengthPrefixEncode(cmd.toBytes)
    val write = Tcp.Write(bytes, Ack)
    connection ! write

    {
      case Tcp.WritingResumed ⇒ connection ! write
      case t@Tcp.CommandFailed(_: Tcp.Write) ⇒ connection ! ResumeWriting
      case Ack ⇒
        firstSent = true
        connection ! ResumeReading
        tryPushDownstream()
        context.become(materialized)

      case anyElse ⇒ log.warning("received unexpected {} when in connnected state", anyElse)
    }
  }

  // halfReady signals when one of the async events (Connected, Request)
  // that we're waiting for has been received already
  def idle: Receive = commonBehaviour orElse {

    case f@Tcp.CommandFailed(_: Connect) ⇒
      log.tylog(Level.DEBUG, traceId, CreateTcpConnection,
        Variation.Failure(new Exception(f.cmd.failureMessage.toString)), "failed to obtain new tcp connection")
      context.become(terminating(StreamCompleted.error(traceId, new Exception(f.cmd.failureMessage.toString))))

    case ActorPublisherMessage.SubscriptionTimeoutExceeded ⇒
      log.info(s"no subscriber in within subs timeout $subscriptionTimeout")
      onCompleteThenStop()

    case Tcp.Connected(_, _) ⇒
      log.tylog(Level.DEBUG, traceId, CreateTcpConnection, Variation.Success, "received tcp connection")
      connection = sender()
      context watch connection
      context.become(connected)

    case ActorPublisherMessage.Request(_) ⇒ //ignore for now

    case anyElse ⇒ log.warning("received unexpected {} when in idle state", anyElse)
  }

  override def receive: Receive = idle

}