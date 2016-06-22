package build.unstable.sonicd.model

import java.net.InetSocketAddress
import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.stage._
import akka.util.ByteString
import build.unstable.tylog.Variation
import JsonProtocol._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object SonicdSource extends SonicdLogging {


  /* HELPERS */

  class IncompleteStreamException extends Exception("stream was closed before done event was sent")

  case class SonicProtocolStage(traceId: String)
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

        var first: Boolean = true

        var last: Option[DoneWithQueryExecution] = None

        def complete() = {
          last match {
            case Some(l) ⇒
              if (l.success) completeStage()
              else if (l.error.nonEmpty) {
                failStage(l.error.get)
              } else failStage(new Exception("protocol error: done event is not success but errors is empty"))
            case None ⇒
              failStage(new IncompleteStreamException)
          }
        }

        def pushAck() = {
          push(out1, close.get)
          close = None
        }

        //conn in
        setHandler(in1, new InHandler {

          @throws[Exception](classOf[Exception])
          override def onUpstreamFailure(ex: Throwable): Unit = failStage(ex)

          @throws[Exception](classOf[Exception])
          override def onUpstreamFinish(): Unit =
            if (last.isEmpty) failStage(new IncompleteStreamException)
            else super.onUpstreamFinish()

          override def onPush(): Unit = {
            val elem = grab(in1)
            if (first) {
              trace(log, traceId, EstablishCommunication, Variation.Success, "received first message from gateway")
              first = false
            }
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

        //client in
        setHandler(in2, new InHandler {

          @throws[Exception](classOf[Exception])
          override def onUpstreamFailure(ex: Throwable): Unit = super.onUpstreamFailure(ex)

          @throws[Exception](classOf[Exception])
          override def onUpstreamFinish(): Unit = {
            //dont complete here as we want to make sure that tcp connection
            //stays open even if TCP Half-Close mechanism is not enabled
          }

          override def onPush(): Unit = {
            val elem = grab(in2)
            elem match {
              case q: SonicCommand ⇒
                trace(log, traceId, EstablishCommunication, Variation.Attempt, "sending first message to gateway")
                val bytes = lengthPrefixEncode(q.toBytes)
                push(out1, bytes)
              case msg ⇒
                val bytes = lengthPrefixEncode(msg.toBytes)
                push(out1, bytes)
            }
          }
        })

        //conn out
        setHandler(out1, new OutHandler {

          @throws[Exception](classOf[Exception])
          override def onDownstreamFinish(): Unit = super.onDownstreamFinish()

          override def onPull(): Unit = {
            if (last.isDefined) {
              pushAck()
              complete()
            } else if (!hasBeenPulled(in2) && !isClosed(in2)) {
              pull(in2)
            }
          }
        })

        //client outnew
        setHandler(out2, new OutHandler {

          @throws[Exception](classOf[Exception])
          override def onDownstreamFinish(): Unit = super.onDownstreamFinish()

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

  def logGraphBuild[T](msg: SonicCommand)(f: SonicCommand ⇒ T): T = {
    val (withTraceId, traceId) = msg.traceId match {
      case Some(i) ⇒ msg → i
      case None ⇒
        val id = UUID.randomUUID().toString
        msg.setTraceId(id) -> id
    }

    trace(log, traceId, BuildGraph, Variation.Attempt, "building graph command {}", msg)
    val graph = f(withTraceId)
    trace(log, traceId, BuildGraph, Variation.Success, "materialized graph {}", traceId)
    graph
  }

  def logConnectionCreate(traceId: String)(f: Future[Tcp.OutgoingConnection])
                         (implicit ctx: ExecutionContext): Future[Tcp.OutgoingConnection] = {
    trace(log, traceId, CreateTcpConnection, Variation.Attempt, "create new tcp connection")
    f.andThen {
      case Success(i) ⇒ trace(log, traceId, CreateTcpConnection, Variation.Success, "created new tcp connection")
      case Failure(e) ⇒ trace(log, traceId, CreateTcpConnection, Variation.Failure(e), "failed to create tcp connection")
    }
  }

  //FIXME seems to complete when tcp connection throws exception
  private final def runCommand(query: SonicCommand,
                               connection: Flow[ByteString, ByteString, Future[Tcp.OutgoingConnection]])
                              (implicit system: ActorSystem, mat: ActorMaterializer): Future[Vector[SonicMessage]] = {


    logGraphBuild(query) { withTraceId ⇒
      val foldMessages = Sink.fold[Vector[SonicMessage], SonicMessage] (Vector.empty) (_:+ _)

      RunnableGraph.fromGraph(GraphDSL.create(foldMessages) {
        implicit b ⇒
          fold ⇒

            import GraphDSL.Implicits._
            val traceId = withTraceId.traceId.get

            val q = b.add(Source.single(withTraceId))
            val conn = b.add(connection.mapMaterializedValue(logConnectionCreate(traceId)(_)(system.dispatcher)))
            val protocol = b.add(SonicProtocolStage(traceId))
            val framing = b.add(framingStage)

            q ~> protocol.in2
            protocol.out1 ~> conn
            protocol.in1 <~ framing <~ conn
            protocol.out2 ~> fold

            ClosedShape
      }).run()
    }
  }


  /* API */

  /**
   * runs the given query against the sonicd instance
   * in address
   *
   * @param address sonicd instance address
   * @param query query to run
   */
  def run(query: Query, address: InetSocketAddress)
         (implicit system: ActorSystem, mat: ActorMaterializer): Future[Vector[SonicMessage]] =
    run(query, Tcp().outgoingConnection(address))

  def run(query: Query,
          connection: Flow[ByteString, ByteString, Future[Tcp.OutgoingConnection]])
         (implicit system: ActorSystem, mat: ActorMaterializer): Future[Vector[SonicMessage]] = {
    runCommand(query, connection)
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
  final def stream(address: InetSocketAddress, query: Query)
                  (implicit system: ActorSystem): Source[SonicMessage, Future[DoneWithQueryExecution]] = {
    stream(query, Tcp().outgoingConnection(address))
  }

  //FIXME seems to not bubble up exception from tcp connection
  //instead throws NoSuchElement (in Sink.last)
  final def stream(query: Query, connection: Flow[ByteString, ByteString, Future[Tcp.OutgoingConnection]])
                  (implicit system: ActorSystem): Source[SonicMessage, Future[DoneWithQueryExecution]] = {

    import system.dispatcher

    logGraphBuild(query) { withTraceId ⇒
      val last = Sink.last[SonicMessage].mapMaterializedValue {
        f ⇒
          f.map {
            case d: DoneWithQueryExecution ⇒ d
            case m ⇒ DoneWithQueryExecution.error(new Exception(s"protocol error: unknown last message: $m"))
          }
      }

      Source.fromGraph(GraphDSL.create(last) {
        implicit b ⇒
          last ⇒

            import GraphDSL.Implicits._
            val traceId = withTraceId.traceId.get

            val q = b.add(Source.single(withTraceId))
            val conn = b.add(connection.mapMaterializedValue(logConnectionCreate(traceId)(_)(system.dispatcher)))
            val protocol = b.add(SonicProtocolStage(traceId))
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

  /**
   * Authenticates a user and returns a token. Future will fail if api-key is invalid
   */
  def authenticate(user: String, apiKey: String, address: InetSocketAddress)
                  (implicit system: ActorSystem, mat: ActorMaterializer): Future[String] =
    authenticate(user, apiKey, Tcp().outgoingConnection(address))

  def authenticate(user: String, apiKey: String, connection: Flow[ByteString, ByteString, Future[Tcp.OutgoingConnection]])
                  (implicit system: ActorSystem, mat: ActorMaterializer): Future[String] = {
    import mat.executionContext
    runCommand(Authenticate(user, apiKey, None), connection)
      .flatMap(_.find(_.isInstanceOf[OutputChunk])
        .map(o ⇒ o.asInstanceOf[OutputChunk].data.elements.headOption.map(v ⇒ Future(v.convertTo[String]))
          .getOrElse(Future.failed(new Exception("protocol error: output chunk is empty"))))
        .getOrElse(Future.failed(new Exception("protocol error: no output chunk in result"))))
  }
}
