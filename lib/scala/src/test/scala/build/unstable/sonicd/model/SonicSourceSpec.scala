package build.unstable.sonicd.model

import java.net.InetSocketAddress

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import build.unstable.sonicd.model.SonicdSource.{IncompleteStreamException, SonicProtocolStage}
import org.scalatest.concurrent._
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Future

class SonicSourceSpec extends WordSpecLike with Matchers with BeforeAndAfterAll with ScalaFutures {

  val tcpError = "TCPBOOM"

  class TcpException extends Exception(tcpError)

  implicit val system = ActorSystem(this.getClass.getSimpleName)
  implicit val mat = ActorMaterializer(ActorMaterializerSettings(system))

  val clientProtocol: Graph[BidiShape[ByteString, ByteString, SonicMessage, SonicMessage], _] =
    new SonicProtocolStage()

  val tcpFailure1: Flow[ByteString, ByteString, Future[Tcp.OutgoingConnection]] =
    Flow.fromSinkAndSource(Sink.ignore, Source.failed(new TcpException))
      .mapMaterializedValue(_ ⇒
        Future.successful(Tcp.OutgoingConnection(new InetSocketAddress(1), new InetSocketAddress(2))))

  val tcpFailure2: Flow[ByteString, ByteString, Future[Tcp.OutgoingConnection]] =
    tcpFailure1.mapMaterializedValue(_ ⇒ Future.failed[Tcp.OutgoingConnection](new TcpException))

  val noFraming: Flow[ByteString, ByteString, NotUsed] = Flow[ByteString].map(a ⇒ a)

  val foldMessages: Sink[SonicMessage, Future[Vector[SonicMessage]]] =
    Sink.fold[Vector[SonicMessage], SonicMessage](Vector.empty)(_ :+ _)

  override protected def afterAll(): Unit = {
    system.terminate()
  }

  def runTestGraph(queryStage: Source[SonicMessage, NotUsed],
                   connectionStage: Flow[ByteString, ByteString, Future[Tcp.OutgoingConnection]],
                   framingStage: Flow[ByteString, ByteString, NotUsed],
                   lastStage: Sink[SonicMessage, Future[Vector[SonicMessage]]]): Future[Vector[SonicMessage]] = {

    RunnableGraph.fromGraph(GraphDSL.create(lastStage) { implicit b ⇒
      last ⇒
        import GraphDSL.Implicits._

        val conn = b.add(connectionStage)
        val protocol = b.add(clientProtocol)
        val framing = b.add(framingStage)
        val query = b.add(queryStage)

        query ~> protocol.in2
        protocol.out1 ~> conn
        protocol.in1 <~ framing <~ conn
        protocol.out2 ~> last

        ClosedShape

    }).run()
  }

  "SonicProtocolStage" should {
    "bubble up exception correctly if upstream connection stage fails" in {
      val f1 = runTestGraph(Source.empty, tcpFailure1, noFraming, foldMessages)
      whenReady(f1.failed) { ex ⇒
        ex shouldBe an[TcpException]
        ex.getMessage shouldBe tcpError
      }
      val f2 = runTestGraph(Source.empty, tcpFailure2, noFraming, foldMessages)
      whenReady(f2.failed) { ex ⇒
        ex shouldBe an[TcpException]
        ex.getMessage shouldBe tcpError
      }
    }
    "fail if upstream connection closes before Done event" in {
      val tcpFlow: Flow[ByteString, ByteString, Future[Tcp.OutgoingConnection]] =
        Flow.fromSinkAndSource[ByteString, ByteString](
        Sink.ignore, Source.single(SonicdSource.lengthPrefixEncode(QueryProgress(Some(100), None).toBytes)))
          .mapMaterializedValue(_ ⇒ Future.successful(
            Tcp.OutgoingConnection(new InetSocketAddress(1), new InetSocketAddress(2))))

      val f = runTestGraph(Source.empty, tcpFlow, noFraming, foldMessages)
      whenReady(f.failed) { ex ⇒
        ex shouldBe an[IncompleteStreamException]
      }
    }
  }

  "run graph method" should {
    "bubble up exceptions correctly if upstream connection stage fails" in {
      val f1 = SonicdSource.run(Fixture.syntheticQuery, tcpFailure1)
      whenReady(f1.failed) { ex ⇒
        ex shouldBe an[TcpException]
        ex.getMessage shouldBe tcpError
      }
      val f2 = SonicdSource.run(Fixture.syntheticQuery, tcpFailure2)
      whenReady(f2.failed) { ex ⇒
        ex shouldBe an[TcpException]
        ex.getMessage shouldBe tcpError
      }
    }
  }

  "stream graph method" should {
    "bubble up exceptions correctly if upstream connection stage fails" in {
      val f1 = SonicdSource.stream(Fixture.syntheticQuery, tcpFailure1).to(Sink.ignore).run()
      whenReady(f1.failed) { ex ⇒
        ex shouldBe an[TcpException]
        ex.getMessage shouldBe tcpError
      }
      val f2 = SonicdSource.stream(Fixture.syntheticQuery, tcpFailure2).to(Sink.ignore).run()
      whenReady(f2.failed) { ex ⇒
        ex shouldBe an[TcpException]
        ex.getMessage shouldBe tcpError
      }
    }
  }
}
