package build.unstable.sonicd.examples

import java.net.InetSocketAddress
import java.util.UUID

import akka.actor._
import akka.stream.scaladsl.Sink
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import build.unstable.sonicd.model._
import spray.json._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/**
 * Makes use of `sonicd-core` artifact which provides a convenience
 * constructor for a [[akka.stream.scaladsl.Source]].
 */
object AkkaExample extends App {

  val config: JsObject = """{"class" : "SyntheticSource"}""".parseJson.asJsObject
  val addr = new InetSocketAddress("127.0.0.1", 10001)

  implicit val system = ActorSystem()
  implicit val materializer: ActorMaterializer =
    ActorMaterializer(ActorMaterializerSettings(system))

  val queryId1 = UUID.randomUUID().toString
  val query1 = Query("100", config, None).copy(trace_id = Some(queryId1))

  val queryId2 = UUID.randomUUID().toString
  val query2 = Query("10", config, None).copy(trace_id = Some(queryId2))

  val res1: Future[DoneWithQueryExecution] = SonicdSource.stream(addr, query1).to(Sink.ignore).run()

  val res2: Future[Vector[SonicMessage]] = SonicdSource.run(query2, addr)

  val done1 = Await.result(res1, 20.seconds)
  val done2 = Await.result(res2, 20.seconds)

  //assert(done1.success)
  assert(done2.length == 112) //1 metadata + 100 QueryProgress + 10 OutputChunk + 1 DoneWithQueryExecution

  system.terminate()

}
