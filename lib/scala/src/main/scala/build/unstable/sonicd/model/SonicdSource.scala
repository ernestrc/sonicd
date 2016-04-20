package build.unstable.sonicd.model

import java.net.InetSocketAddress
import java.nio.{ByteBuffer, ByteOrder}

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.stage._
import akka.util.ByteString

import scala.concurrent.Future

object SonicdSource {

  class IncompleteStreamException extends Exception("stream was closed before completion")

  class SonicProtocolStage
    extends GraphStage[BidiShape[ByteString, ByteString, SonicMessage, SonicMessage]] {
    val in1: Inlet[ByteString] = Inlet("ServerIncoming")
    val out2: Outlet[SonicMessage] = Outlet("ClientOutgoing")
    val in2: Inlet[SonicMessage] = Inlet("ClientIncoming")
    val out1: Outlet[ByteString] = Outlet("ServerOutgoing")

    override val shape: BidiShape[ByteString, ByteString, SonicMessage, SonicMessage] =
      BidiShape(in1, out1, in2, out2)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) {

        var close: Option[ByteString] =
          Some(lengthPrefixEncode(ClientAcknowledge.toBytes))

        var last: Option[DoneWithQueryExecution] = None

        def complete() = {
          val l = last.get
          if (l.success) completeStage()
          else if (l.errors.nonEmpty) {
            failStage(l.errors.head)
          } else failStage(new Exception("there was an unexpected error in sonicd"))
        }

        def pushAck() = {
          push(out1, close.get)
          close = None
        }

        setHandler(in1, new InHandler {

          @throws[Exception](classOf[Exception])
          override def onUpstreamFinish(): Unit =
            if (last.isEmpty) failStage(new IncompleteStreamException)
            else super.onUpstreamFinish()

          override def onPush(): Unit = {
            val elem = grab(in1)
            val msg = SonicMessage.fromBytes(elem.splitAt(4)._2)
            push(out2, msg)
            if (msg.isDone) {
              last = Some(msg.asInstanceOf[DoneWithQueryExecution])
              if (isAvailable(out1) && !isClosed(out1)) {
                pushAck()
                complete()
              }
            }
          }
        })

        setHandler(in2, new InHandler {
          @throws[Exception](classOf[Exception])
          override def onUpstreamFinish(): Unit =
            if (last.isDefined) {
              pushAck()
              complete()
            }

          override def onPush(): Unit = {
            val elem = grab(in2)
            val bytes = lengthPrefixEncode(elem.toBytes)
            push(out1, bytes)
          }
        })

        setHandler(out1, new OutHandler {

          override def onPull(): Unit = {
            if (last.isDefined) {
              pushAck()
              complete()
            } else if (!hasBeenPulled(in2) && !isClosed(in2)) {
              pull(in2)
            }
          }
        })

        setHandler(out2, new OutHandler {
          override def onPull(): Unit = {
            pull(in1)
          }
        })
      }
  }

  //length prefix framing
  def lengthPrefixEncode(bytes: ByteString): ByteString = {
    val len = ByteBuffer.allocate(4)
    len.putInt(bytes.length)
    ByteString(len.array() ++ bytes)
  }

  val framingStage =
    Flow[ByteString]
      .via(Framing.lengthField(
        fieldLength = 4,
        fieldOffset = 0,
        maximumFrameLength = 1000000 /* 1 MB */ ,
        byteOrder = ByteOrder.BIG_ENDIAN)
      )

  /**
   * runs the given query against the sonicd instance
   * in address
   *
   * @param address sonicd instance address
   * @param query query to run
   */
  def run(address: InetSocketAddress, query: Query)
         (implicit system: ActorSystem, mat: ActorMaterializer): Future[Vector[SonicMessage]] = {
    run(query, Tcp().outgoingConnection(address, halfClose = false))
  }

  def run(query: Query,
          connection: Flow[ByteString, ByteString, Future[Tcp.OutgoingConnection]])
         (implicit system: ActorSystem, mat: ActorMaterializer): Future[Vector[SonicMessage]] = {

    val foldMessages = Sink.fold[Vector[SonicMessage], SonicMessage](Vector.empty)(_ :+ _)

    RunnableGraph.fromGraph(GraphDSL.create(foldMessages) { implicit b ⇒
      fold ⇒
        import GraphDSL.Implicits._

        val conn = b.add(connection)
        val protocol = b.add(new SonicProtocolStage)
        val framing = b.add(framingStage)
        val q = b.add(Source.single(query))

        q ~> protocol.in2
        protocol.out1 ~> conn
        protocol.in1 <~ framing <~ conn
        protocol.out2 ~> fold

        ClosedShape
    }).run()
  }

  /**
   * builds [[akka.stream.scaladsl.Source]] of SonicMessage.
   * The materialized value corresponds to the number of messages
   * that were streamed and signals when the stream
   * is completed if it's a finite stream.
   *
   * @param address sonicd instance address
   * @param query query to run
   */
  def stream(address: InetSocketAddress, query: Query)
            (implicit system: ActorSystem): Source[SonicMessage, Future[DoneWithQueryExecution]] = {
    stream(query, Tcp().outgoingConnection(address, halfClose = false))
  }

  def stream(query: Query, connection: Flow[ByteString, ByteString, Future[Tcp.OutgoingConnection]])
            (implicit system: ActorSystem): Source[SonicMessage, Future[DoneWithQueryExecution]] = {

    import system.dispatcher

    val last = Sink.last[SonicMessage].mapMaterializedValue { f ⇒
      f.map {
        case d: DoneWithQueryExecution ⇒ d
        case m ⇒ DoneWithQueryExecution.error(new Exception(s"protocol error: unknown last message: $m"))
      }
    }

    Source.fromGraph(GraphDSL.create(last) { implicit b ⇒
      last ⇒
        import GraphDSL.Implicits._

        val q = b.add(Source.single(query))
        val conn = b.add(connection)
        val protocol = b.add(new SonicProtocolStage())
        val framing = b.add(framingStage)
        val bcast = b.add(Broadcast[SonicMessage](2))

        q ~> protocol.in2
        protocol.out1 ~> conn
        protocol.in1 <~ framing <~ conn
        protocol.out2 ~> bcast
        bcast.out(1) ~> last

        SourceShape(bcast.out(0))
    })
  }
}
