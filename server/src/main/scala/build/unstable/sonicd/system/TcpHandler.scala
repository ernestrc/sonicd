package build.unstable.sonicd.system

import akka.actor.SupervisorStrategy.Stop
import akka.actor._
import akka.io.Tcp
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorSubscriberMessage.OnNext
import akka.util.ByteString
import build.unstable.sonicd.model.Exceptions.ProtocolException
import build.unstable.sonicd.model._
import build.unstable.sonicd.system.SonicController.NewQuery
import org.reactivestreams.{Subscriber, Subscription}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

class TcpSupervisor(controller: ActorRef) extends Actor with ActorLogging {

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    log.info(s"tcp supervisor $self is up and awaiting Tcp.Bound message")
  }

  override def supervisorStrategy: SupervisorStrategy =
    OneForOneStrategy(loggingEnabled = true) {
      case e: Exception ⇒ Stop
    }

  def listening(listener: ActorRef): Receive = {

    case c@Tcp.Connected(remote, local) =>
      val connection = sender()
      val handler = context.actorOf(Props(classOf[TcpHandler], controller, connection))
      connection ! Tcp.Register(handler, keepOpenOnPeerClosed = true)
      listener ! Tcp.ResumeAccepting(1)
  }

  override def receive: Actor.Receive = {

    case Tcp.CommandFailed(_: Tcp.Bind) => context stop self

    case b@Tcp.Bound(localAddress) =>
      log.info(s"ready and listening for new connections on $localAddress")
      val listener = sender()
      listener ! Tcp.ResumeAccepting(1)
      context.become(listening(listener))

  }
}

object TcpHandler {

  case class Ack(offset: Long) extends Tcp.Event

  case object CompletedStream

}

class TcpHandler(controller: ActorRef, connection: ActorRef)
  extends Actor with ActorLogging {

  import akka.io.Tcp._
  import TcpHandler._
  import context.dispatcher

  //wrapper around org.reactivestreams.Subscription
  class StreamSubscription(s: Subscription) {
    var requested: Long = 0L

    def request(n: Long) = {
      requested += n
      s.request(n)
    }
  }

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case e: Exception ⇒
      log.error(e, "error in publisher")
      self ! DoneWithQueryExecution.error(e)
      Stop
  }

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    log.debug(s"starting tcp handler in path '{}'", self.path)
    connection ! ResumeReading
  }


  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    if (cancellableCompleted != null) cancellableCompleted.cancel()
    log.debug(s"stopped tcp handler in path '{}'. transferred {} events", self.path, transferred)
  }

  def closing(ev: DoneWithQueryExecution): Receive = framing(deserializeAndHandleClientAcknowledgeFrame) orElse {
    log.debug("switched to closing behaviour with ev {} and storage {}", ev, storage)
    // check if we're ready to send done msg
    if (storage.isEmpty) {
      buffer(ev)
      writeOne()
    } else buffer(ev)

    val recv: Receive = {
      case ConfirmedClosed ⇒ context.stop(self)
      case CommandFailed(_: Write) => connection ! ResumeWriting
      case WritingResumed => writeOne()
      case Ack(offset) =>
        acknowledge(offset)
        if (storage.length > 0) writeOne()
    }
    recv
  }

  val subs = new Subscriber[SonicMessage] {

    override def onError(t: Throwable): Unit = {
      log.error(t, "stream error")
      self ! DoneWithQueryExecution.error(t)
    }

    override def onSubscribe(s: Subscription): Unit = {
      self ! s
    }

    override def onComplete(): Unit = {
      log.debug("stream is complete")
      cancellableCompleted = context.system.scheduler.scheduleOnce(2.seconds, self, CompletedStream)
    }

    override def onNext(t: SonicMessage): Unit = {
      //log.debug("sending msg to self")
      self ! t
    }
  }

  def buffer(t: SonicMessage) = {
    currentOffset += 1
    //length prefix framing
    val w = Write(SonicdSource.lengthPrefixEncode(t.toBytes), Ack(currentOffset))
    storage.append(w)
    //log.debug("buffered {}", w)
    w
  }

  private def writeOne(): Unit = {
    val msg = storage.head
    connection ! storage.head
    //log.debug("sending for write {}", msg)
  }

  private def acknowledge(offset: Long): Unit = {
    if (storage.nonEmpty) {

      if (offset > transferred) {
        transferred += 1
        val idx = offset - transferred

        //log.debug("acknowledge offset {} transferred {} therefore ack {}", offset, transferred, idx)

        storage.remove(idx.toInt)
      } else log.warning("received double ack: {}", offset)
    } else {
      log.warning(s"storage was empty at ack $offset")
    }
  }

  context watch connection

  val storage = ListBuffer.empty[Write]
  var currentOffset = 0
  var transferred = 0
  var handler = context.system.deadLetters
  var subscription: StreamSubscription = null
  var cancellableCompleted: Cancellable = null
  var dataBuffer = ByteString.empty

  def commonBehaviour: Receive = {

    case ConfirmedClosed ⇒ context.stop(self)

    case ev: DoneWithQueryExecution ⇒
      log.debug("received done msg")
      context.become(closing(ev))

    case CompletedStream ⇒
      val msg = "completed stream without done msg"
      log.error(msg)
      context.become(closing(DoneWithQueryExecution.error(new ProtocolException(msg))))

    case PeerClosed => log.debug("peer closed")
  }

  def deserializeAndHandleQueryFrame(data: ByteString): Unit = {
    SonicMessage.fromBytes(data) match {
      case q: Query ⇒ controller ! NewQuery(q)
      case ClientAcknowledge ⇒ connection ! ConfirmedClose
      case anyElse ⇒ throw new Exception(s"protocol error $anyElse not expected")
    }
  }

  def deserializeAndHandleClientAcknowledgeFrame(data: ByteString): Unit =
    SonicMessage.fromBytes(data) match {
      case ClientAcknowledge ⇒ connection ! ConfirmedClose
      case anyElse ⇒ //ignore
    }

  def deserializeAndHandleMessageFrame(data: ByteString): Unit =
    SonicMessage.fromBytes(data) match {
      case ClientAcknowledge ⇒ connection ! ConfirmedClose
      case anyElse ⇒ handler ! OnNext(anyElse)
    }

  def streaming: Receive =
    commonBehaviour orElse framing(deserializeAndHandleMessageFrame) orElse {
      case t: SonicMessage ⇒
        buffer(t)
        writeOne()

        context.become(commonBehaviour orElse {
          case t: SonicMessage ⇒ buffer(t)
          case Ack(offset) ⇒
            acknowledge(offset)
            if (storage.isEmpty) {
              subscription.request(1)
              context.unbecome()
            } else writeOne()
          case WritingResumed ⇒ writeOne()
          case CommandFailed(Write(_, _)) =>
            connection ! ResumeWriting
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
          log.error(e, "error framing incoming bytes")
          context.become(closing(DoneWithQueryExecution.error(e)))
      } finally {
        connection ! ResumeReading
      }
  }

  def receive: Receive = framing(deserializeAndHandleQueryFrame) orElse commonBehaviour orElse {
    case s: Subscription ⇒
      //start streaming
      log.debug("subscribed to publisher, requesting first element")
      subscription = new StreamSubscription(s)
      subscription.request(1)
      context.become(streaming)

    case handlerProps: Props ⇒
      log.debug("received props {}", handlerProps)
      handler = context.actorOf(handlerProps)
      val pub = ActorPublisher[SonicMessage](handler)
      pub.subscribe(subs)
  }
}
