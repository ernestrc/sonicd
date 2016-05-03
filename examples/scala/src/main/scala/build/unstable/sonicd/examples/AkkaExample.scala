package build.unstable.sonicd.examples

import java.net.InetSocketAddress

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
  val query = Query("10", config)
  val addr = new InetSocketAddress("127.0.0.1", 10001)

  implicit val system = ActorSystem()
  implicit val materializer: ActorMaterializer =
    ActorMaterializer(ActorMaterializerSettings(system))

  val stream: Future[DoneWithQueryExecution] =
    SonicdSource.stream(addr, query).to(Sink.ignore).run()

  val future: Future[Vector[SonicMessage]] =
    SonicdSource.run(addr, query)

  val fDone = Await.result(future, 20.seconds)
  val sDone = Await.result(stream, 20.seconds)

  assert(sDone.success)
  assert(fDone.length == 112) //1 metadata + 100 QueryProgress + 10 OutputChunk + 1 DoneWithQueryExecution

  system.terminate()

}
