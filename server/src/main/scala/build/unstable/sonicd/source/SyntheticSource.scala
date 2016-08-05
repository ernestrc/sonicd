package build.unstable.sonicd.source

import akka.actor._
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import build.unstable.sonicd.model.JsonProtocol._
import build.unstable.sonicd.model.{DataSource, Query, RequestContext, SonicMessage}
import spray.json._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.{Random, Try}

class SyntheticSource(query: Query, actorContext: ActorContext, context: RequestContext)
  extends DataSource(query, actorContext, context) {

  val handlerProps: Props = {
    val seed = getOption[Int]("seed")
    val size = getOption[Int]("size")
    val progress = getOption[Int]("progress-delay").getOrElse(10)
    val indexed = getOption[Boolean]("indexed").getOrElse(false)

    //user pre-defined schema
    val schema = getOption[JsObject]("schema")

    Props(classOf[SyntheticPublisher], query.id.get, seed, size,
      progress, query.query, indexed, schema, context)
  }
}

class SyntheticPublisher(queryId: Long, seed: Option[Int], size: Option[Int], progressWait: Int,
                         query: String, indexed: Boolean, schema: Option[JsObject], ctx: RequestContext)
  extends Actor with ActorPublisher[SonicMessage] with ActorLogging {

  import SyntheticPublisher._
  import build.unstable.sonicd.model._

  //in case this publisher never gets subscribed to
  override def subscriptionTimeout: Duration = 10.seconds

  val rdm = seed.map(s ⇒ new Random(s)).getOrElse(new Random())

  var streamed = 0L
  val preTarget = 101
  //+100 of progress +1 metadata
  val _query = Try(query.trim().toInt)
  val target =
    _query
      .recoverWith {
        case e: Exception ⇒
          log.warning("could not parse query to determine test target size")
          Try(size.get)
      }.toOption.map(_ + preTarget)

  val data = target.map(t ⇒ (0 until t).map(_ ⇒ genOne()).to[mutable.Queue])

  // to test source unexpected exceptions
  // pass query negative integer
  if (_query.isSuccess) {
    assert(_query.get > 0)
  }

  // to test expected exception
  // pass query or size '28'
  val shouldThrowExpectedException = _query.isSuccess && _query.get == 28

  if (shouldThrowExpectedException) log.warning("this source will throw an expected exception")

  // if string is empty, value is 0 or bool is true, randomize values
  // otherwise use the value provided in the schema
  def genRandom(value: JsValue): JsValue = value match {
    case JsString(str) if str.contains(ENUM) ⇒
      val enum = str.split(ENUM)
      JsString(enum(rdm.nextInt(enum.length)))
    case JsString("") ⇒ JsString(LOREM.takeRight(rdm.nextInt(LOREM.length)).take(10).replace(" ", ""))
    case JsBoolean(true) ⇒ JsBoolean(rdm.nextBoolean())
    case j@JsNumber(i) if j.value.doubleValue() == 0d ⇒ JsNumber(rdm.nextInt())
    case JsNull ⇒ JsNull

    case JsString(str) ⇒ JsString(str)
    case JsBoolean(bo) ⇒ JsBoolean(bo)
    case JsNumber(nu) ⇒ JsNumber(nu)
    case JsObject(f) ⇒ JsObject(randomFromSchema(f))
    case JsArray(v) ⇒ JsArray(v.map(p ⇒ genRandom(p)))
  }

  def randomFromSchema(fields: Map[String, JsValue]): Map[String, JsValue] = {
    fields.map(kv ⇒ kv._1 → genRandom(kv._2))
  }

  def genOne(): OutputChunk = {
    if (schema.isDefined) {
      val payload = randomFromSchema(schema.get.fields)
      OutputChunk(JsArray(payload.values.toVector))
    } else if (indexed) {
      OutputChunk(JsArray(JsString(streamed.toString), JsNumber(rdm.nextInt())))
    } else OutputChunk(Vector(rdm.nextInt()))
  }

  @tailrec
  private def stream(demand: Long): Unit = {
    if (totalDemand > 0) {
      if (data.isDefined) {
        onNext(data.get.dequeue())
      } else {
        onNext(genOne())
      }
      streamed += 1
      stream(demand - 1L)
    }
  }

  @tailrec
  private def progress(demand: Long): Unit = {
    if (totalDemand > 0L && isActive) {
      if (streamed < preTarget) {
        Thread.sleep(progressWait)
        if (streamed + 1 == preTarget) onNext(QueryProgress(QueryProgress.Finished, 1, Some(100), Some("%")))
        else onNext(QueryProgress(QueryProgress.Running, 1, Some(100), Some("%")))
        streamed += 1
        progress(demand - 1L)
      } else {
        self ! Request(demand)
      }
    }
  }

  def receive: Receive = {

    //on the 10th message, if shouldThrowControlledException
    case Request(n) if shouldThrowExpectedException && streamed == 111L ⇒
      onNext(DoneWithQueryExecution.error(new Exception("controlled exception test")))
      onCompleteThenStop()

    case Request(n) if streamed == 0L ⇒
      log.info(s"starting synthetic stream with target of '$target'")
      lazy val m = TypeMetadata(Vector("data" → JsNumber(0)))

      if (schema.isDefined) {
        onNext(TypeMetadata(schema.get.fields.toVector))
      } else if (indexed) {
        onNext(m.copy(typesHint = Vector("index" → JsNumber(0)) ++ m.typesHint))
      } else {
        onNext(m)
      }
      streamed += 1
      progress(n)

    case Request(n) if streamed < preTarget ⇒ progress(n)

    case Request(n) if target.nonEmpty && streamed >= target.get ⇒
      log.info(s"reached target of ${target.get - preTarget}")
      onNext(DoneWithQueryExecution.success)
      onCompleteThenStop()

    case Request(n) ⇒ stream(n)

    case Cancel ⇒
      log.debug("client canceled")
      onCompleteThenStop()
  }
}

object SyntheticPublisher {

  val ENUM = '|'
  val LOREM = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."
}
