package build.unstable.sonicd.system.actor

import java.net.InetAddress
import java.util.UUID

import akka.actor.SupervisorStrategy.Stop
import akka.actor._
import akka.io.Tcp
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorSubscriberMessage.OnNext
import akka.util.ByteString
import build.unstable.sonic.Exceptions.ProtocolException
import build.unstable.sonic.JsonProtocol._
import build.unstable.sonic._
import build.unstable.sonicd.SonicdLogging
import build.unstable.sonicd.model.StreamSubscription
import build.unstable.tylog.Variation
import org.reactivestreams.{Subscriber, Subscription}

import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

class TcpSupervisor(controller: ActorRef, authService: ActorRef)
  extends Actor with SonicdLogging {

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    info(log, "tcp supervisor {} is up and awaiting Tcp.Bound message", self)
  }

  override def supervisorStrategy: SupervisorStrategy =
    OneForOneStrategy(loggingEnabled = true) {
      case NonFatal(_) ⇒ Stop
    }

  def listening(listener: ActorRef): Receive = {

    case c@Tcp.Connected(remote, local) =>
      debug(log, "new connection: remoteAddr: {}; localAddr: {}", remote, local)
      val connection = sender()
      val handler = context.actorOf(Props(classOf[TcpHandler], controller,
        authService, connection, remote.getAddress))
      connection ! Tcp.Register(handler)
      listener ! Tcp.ResumeAccepting(1)
  }

  override def receive: Actor.Receive = {

    case Tcp.CommandFailed(_: Tcp.Bind) ⇒ context stop self

    case b@Tcp.Bound(localAddress) ⇒
      info(log, "ready and listening for new connections on {}", localAddress)
      val listener = sender()
      listener ! Tcp.ResumeAccepting(1)
      context.become(listening(listener))

  }
}

object TcpHandler {

  case class Ack(offset: Long) extends Tcp.Event

  case object CompletedStream

}

class TcpHandler(controller: ActorRef, authService: ActorRef,
                 connection: ActorRef, clientAddress: InetAddress)
  extends Actor with SonicdLogging with Stash {

  import TcpHandler._
  import akka.io.Tcp._

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case e: Exception ⇒
      error(log, e, "error in publisher")
      self ! StreamCompleted.error(traceId, e)
      Stop
  }

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    debug(log, "starting tcp handler in path {}", self.path)
    connection ! ResumeReading
  }


  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    debug(log, "stopped tcp handler in path '{}'. transferred {} events", self.path, transferred)
  }


  /* HELPERS */

  def buffer(t: SonicMessage) = {
    currentOffset += 1
    //length prefix framing
    val w = Write(Sonic.lengthPrefixEncode(t.toBytes), Ack(currentOffset))
    storage.append(w)
    w
  }

  private def writeOne(): Unit = {
    connection ! storage.head
  }

  private def acknowledge(offset: Long): Unit = {
    if (storage.nonEmpty) {

      if (offset > transferred) {
        transferred += 1
        val idx = offset - transferred

        //log.debug("acknowledge offset {} transferred {} therefore ack {}", offset, transferred, idx)

        storage.remove(idx.toInt)
      } else warning(log, "received double ack: {}", offset)
    } else {
      warning(log, "storage was empty at ack {}", offset)
    }
  }

  def initState() {
    storage.clear()
    currentOffset = 0
    transferred = 0
    handler = context.system.deadLetters
    subscription = null
    dataBuffer = ByteString.empty
    traceId = UUID.randomUUID().toString
  }

  def restartInternally(): Unit = {
    initState()
    unstashAll()
    context.become(receive)
  }

  val subs = new Subscriber[SonicMessage] {

    override def onError(t: Throwable): Unit = {
      error(log, t, "stream error")
      self ! StreamCompleted.error(traceId, t)
    }

    override def onSubscribe(s: Subscription): Unit = self ! s

    override def onComplete(): Unit = self ! CompletedStream

    override def onNext(t: SonicMessage): Unit = self ! t
  }


  /* STATE */

  val storage = ListBuffer.empty[Write]
  var currentOffset: Long = _
  var transferred: Long = _
  var handler: ActorRef = _
  var subscription: StreamSubscription = _
  var dataBuffer: ByteString = _
  var traceId: String = _


  /* BEHAVIOUR */

  context watch connection
  initState()

  def closing(ev: StreamCompleted): Receive = framing(stashCommandsHandleAckCancel) orElse {
    debug(log, "switched to closing behaviour with ev {} and storage {}", ev, storage)
    // check if we're ready to send done msg
    if (storage.isEmpty) {
      buffer(ev)
      writeOne()
    } else buffer(ev)

    {
      case PeerClosed ⇒ debug(log, "peer closed")
      case CommandFailed(_: Write) => connection ! ResumeWriting
      case WritingResumed => writeOne()
      case Ack(offset) =>
        acknowledge(offset)
        if (storage.length > 0) writeOne()

      // this can only happen if client cancel msg is delivered
      // after subscribing but before receiving subscription
      case s: Subscription ⇒ s.cancel()
    }
  }

  def commonBehaviour: Receive = {
    case Terminated(_) ⇒ context.stop(self)

    case ev: StreamCompleted ⇒
      log.debug("received done msg")
      context.become(closing(ev))

    case CompletedStream ⇒
      val msg = "completed stream without done msg"
      log.error(msg)
      context.become(closing(StreamCompleted.error(traceId, new ProtocolException(msg))))

    case PeerClosed ⇒ debug(log, "peer closed")
  }

  def handleCommands(data: ByteString): Unit = {
    SonicMessage.fromBytes(data) match {
      case i: SonicCommand ⇒
        val withTraceId = {
          i.traceId match {
            case Some(id) ⇒
              traceId = id
              i
            case None ⇒ i.setTraceId(traceId)
          }
        }
        withTraceId match {
          case q: Query ⇒
            trace(log, withTraceId.traceId.get, MaterializeSource,
              Variation.Attempt, "deserialized query {}", withTraceId)

            controller ! SonicController.NewQuery(q, Some(clientAddress))

          case a: Authenticate ⇒
            trace(log, withTraceId.traceId.get, GenerateToken,
              Variation.Attempt, "deserialized auth cmd {}", withTraceId)

            authService ! withTraceId
        }

        context.become(waiting(withTraceId.traceId.get) orElse commonBehaviour)
      case anyElse ⇒
        val msg = "first message should be a SonicCommand"
        val e = new ProtocolException(msg)
        context.become(closing(StreamCompleted.error(traceId, e)))
    }
  }

  def stashCommandsHandleAckCancel(data: ByteString): Unit =
    SonicMessage.fromBytes(data) match {
      //stash pipelined commands
      case c: SonicCommand ⇒ stash()
      case ClientAcknowledge ⇒ restartInternally()
      case CancelStream ⇒
        storage.clear()
        if (subscription != null) subscription.cancel()
        context.become(closing(StreamCompleted.success(traceId)))
      case anyElse ⇒ //ignore
    }

  def materialized: Receive =
    commonBehaviour orElse framing(stashCommandsHandleAckCancel) orElse {
      case t: SonicMessage ⇒
        buffer(t)
        writeOne()

        context.become(commonBehaviour orElse{
          case t: SonicMessage ⇒ buffer(t)
          case Ack(offset) ⇒
            acknowledge(offset)
            if (storage.isEmpty) {
              subscription.request(1)
              context.unbecome()
            } else writeOne()
          case WritingResumed ⇒ writeOne()
          case Tcp.CommandFailed(Write(_, _)) => connection ! ResumeWriting
        }, discardOld = false)
    }

  def framing(deserialize: (ByteString) ⇒ Unit): Receive = {
    case e@Received(data) ⇒
      log.debug("recv {} bytes", data.size)
      try {
        val receivedData = dataBuffer ++ data
        val (lengthBytes, msgBytes) = receivedData.splitAt(4)
        lengthBytes.asByteBuffer.getInt match {
          case len if len <= msgBytes.length ⇒
            val (fullMsgBytes, rest) = msgBytes.splitAt(len)
            dataBuffer = rest
            deserialize(fullMsgBytes)
          case _ ⇒
            dataBuffer = receivedData
        }
      } catch {
        case e: Exception ⇒
          log.error("error framing incoming bytes", e)
          context.become(closing(StreamCompleted.error(traceId, e)))
      } finally {
        connection ! ResumeReading
      }
  }

  def waiting(traceId: String): Receive =
    framing(stashCommandsHandleAckCancel) orElse {
      //auth cmd failed
      case Failure(e) ⇒
        trace(log, traceId, GenerateToken, Variation.Failure(e), "failed to create token")
        context.become(closing(StreamCompleted.error(traceId, e)))

      //auth cmd succeeded
      case Success(token: AuthenticationActor.Token) ⇒
        trace(log, traceId, GenerateToken, Variation.Success, "successfully generated new token {}", token)
        self ! OutputChunk(Vector(token))
        self ! StreamCompleted.success(traceId)
        context.become(materialized)

      case ev: StreamCompleted ⇒
        context.become(closing(ev))
        trace(log, traceId, MaterializeSource, Variation.Failure(ev.error.get),
          "controller failed to materialize source")

      case s: Subscription ⇒
        val msg = "subscribed to publisher, requesting first element"
        //start streaming
        subscription = new StreamSubscription(s)
        subscription.request(1)
        trace(log, traceId, MaterializeSource, Variation.Success, msg)
        context.become(materialized)

      case handlerProps: Props ⇒
        debug(log, "received props of {}", handlerProps.actorClass())
        handler = context.actorOf(handlerProps)
        val pub = ActorPublisher[SonicMessage](handler)
        pub.subscribe(subs)
    }

  def receive: Receive = framing(handleCommands) orElse commonBehaviour
}
