package build.unstable.sonicd.examples

import java.net.InetSocketAddress
import java.util.UUID

import akka.Done
import akka.actor._
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.util.Timeout
import build.unstable.sonic._
import build.unstable.sonic.client.Sonic
import build.unstable.sonic.model.{Query, SonicMessage}
import spray.json._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/**
 * Makes use of `sonicd-core` artifact which provides a streaming and futures API
 */
object AkkaExample extends App {

  implicit val system = ActorSystem()
  implicit val timeout: Timeout = 15.seconds
  implicit val materializer: ActorMaterializer = ActorMaterializer(ActorMaterializerSettings(system))

  // sonic server address
  val addr = new InetSocketAddress("127.0.0.1", 10001)

  // source configuration
  val config: JsObject = """{"class" : "SyntheticSource"}""".parseJson.asJsObject

  // build a request context
  val traceId = UUID.randomUUID().toString

  // instantiate client, which will allocate resources to query sonic endpoint
  val client = Sonic.Client(addr)

  {
    val query = Query("100", config, traceId, None)

    val source = client.stream(query)
    val sink = Sink.ignore
    val res: Cancellable = source.to(sink).run()

    res.cancel()

    assert(res.isCancelled)
  }

  {
    val query = Query("10", config, traceId, None)

    val res: Future[Vector[SonicMessage]] = client.run(query)

    val done = Await.result(res, 20.seconds)
    assert(done.length == 113) //1 started + 1 metadata + 100 QueryProgress + 10 OutputChunk + 1 DoneWithQueryExecution
  }

  system.terminate()

}
