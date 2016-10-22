package build.unstable.sonicd.source

import akka.NotUsed
import akka.actor.{Actor, ActorContext, ActorRef, Props, Status, Terminated}
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request, SubscriptionTimeoutExceeded}
import akka.stream.scaladsl.{BroadcastHub, Keep, Sink, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, KillSwitches, UniqueKillSwitch}
import build.unstable.sonic.JsonProtocol._
import build.unstable.sonic.model._
import build.unstable.sonicd.source.Kafka.GetBroadcastHub
import build.unstable.sonicd.source.json.JsonUtils
import build.unstable.sonicd.{SonicdConfig, SonicdLogging}
import com.typesafe.config._
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.Deserializer
import spray.json._

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.Try

class KafkaSource(query: Query, actorContext: ActorContext, context: RequestContext)
  extends SonicdSource(query, actorContext, context) {

  import Kafka._

  val settingsMap = getConfig[Map[String, String]]("settings")
  // FIXME maybe this deserializer type should determine underlying kafka publisher
  val keyDeserializer = getOption[Deserializer[String]]("key-deserializer")(Kafka.DeserializerJsonFormat)
  // FIXME
  val valueDeserializer = getOption[Deserializer[String]]("value-deserializer")(Kafka.DeserializerJsonFormat)

  val settings = ConfigFactory.parseMap(settingsMap)
  var consumerSettings = ConsumerSettings(settings, keyDeserializer, valueDeserializer)
  val groupId = settingsMap.get(ConsumerConfig.GROUP_ID_CONFIG)
  val bootstrapServers = settingsMap.getOrElse(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
    throw new Exception(s"missing `${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG}` in `settings`"))

  override def publisher: Props = {
    val supervisor = getSupervisor(bootstrapServers, groupId, actorContext)
    Props(Kafka.getPublisherClass(consumerSettings), supervisor, query, consumerSettings)
  }
}

class KafkaSupervisor(bootstrapServers: String, groupId: Option[String],
                      maxPartitions: Int, bufferSize: Int) extends Actor with SonicdLogging {

  val streams = mutable.Map.empty[String, (UniqueKillSwitch, Source[_, NotUsed])]
  val subscribers = mutable.Map.empty[ActorRef, String]
  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(context.system))

  // FIXME: for now settings are ignored
  // so first query will set the consumer settings
  // and the rest will use the same subscriber
  // even if they have different settings
  // (except bootstrapServers, groupId)
  def getId(topic: String, partition: Option[Int], settings: ConsumerSettings[_, _]): String =
  topic + partition.map(_.toString).getOrElse("")

  override def receive: Receive = {
    case Terminated(ref) ⇒
      subscribers.remove(ref).foreach { id ⇒
        // if there aren't any more subscribers kill graph
        if (subscribers.count(_._2 == id) == 0) {
          streams.remove(id).foreach { case (killSwitch, _) ⇒
            killSwitch.shutdown()
          }
        }
      }
      context unwatch ref

    case GetBroadcastHub(topic, partition, settings) ⇒
      val id = getId(topic, partition, settings)

      val broadcastHub: Source[_, NotUsed] =
        streams.get(id).map(_._2).getOrElse {
          val source = partition.map { p ⇒
            Consumer.plainPartitionedSource(settings, Subscriptions.topics(topic))
              .flatMapMerge(maxPartitions, _._2)
          }.getOrElse {
            Consumer.plainSource(settings, Subscriptions.topics(topic))
          }
          val (killSwitch, broadcast) = source
            .viaMat(KillSwitches.single)(Keep.right)
            .toMat(BroadcastHub.sink(bufferSize))(Keep.both).run()

          streams.update(id, killSwitch → broadcast)
          broadcast
        }

      sender() ! broadcastHub
      // monitor subscriber termination to determine if graph should be terminated
      context watch sender
      subscribers.update(sender, id)
  }
}

// TODO settings guaranteed needs to contain boostrap servers
class KafkaPublisher[K, V](supervisor: ActorRef, query: Query, settings: ConsumerSettings[K, V])(implicit ctx: RequestContext)
  extends ActorPublisher[SonicMessage] with SonicdLogging {

  // messages used with actorRefWithAck method to signal backpressure/completion with
  // underlying kafka consumer
  case object Ack

  case object Started

  case object Completed

  //in case this publisher never gets subscribed to
  override def subscriptionTimeout: Duration = 1.minute

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.debug("stopping kafka publisher of '{}'", ctx.traceId)
  }

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    log.debug("starting kafka publisher of '{}'", ctx.traceId)
  }

  /* HELPERS */

  def tryPushDownstream() {
    while (isActive && totalDemand > 0 && buffer.nonEmpty) {
      onNext(buffer.dequeue())
    }
  }

  // TODO should return select as well so that we can filter on that
  def parseQuery(query: Query): (String, Option[Int], ConsumerRecord[K, V] ⇒ Boolean) = {
    val raw = query.query
    val obj = raw.parseJson.asJsObject(s"query must be a valid JSON object: $raw").fields
    val parsed = JsonUtils.parseQuery(obj)

    val topic: String = {
      val value = Try(obj("topic")).getOrElse(throw new Exception("missing 'topic' in query"))

      Try(value.convertTo[String])
        .getOrElse(throw new Exception("'partition' in query must be an integer"))
    }

    val partition: Option[Int] = obj.get("partition").map(p ⇒ Try(p.convertTo[Int])
      .getOrElse(throw new Exception("'partition' in query must be an integer")))

    (topic, partition, { record ⇒ parsed.valueFilter(parseRecord(record))})
  }

  def parseRecord(c: ConsumerRecord[K, V]): Map[String, JsValue] = {
    // FIXME we need to pass a sonic serializer in the class path
    // capable of serializing the value into something more interesting
    // maybe the best way is to follow a model like LocalFileSource in where
    // there is a concrete class like KafkaJsonSource
    Map("key" → JsString(c.key.toString), "value" → JsString(c.value.toString))
  }

  /* STATE */

  var bufferedMeta: Boolean = false
  var pendingAck: Boolean = false
  val buffer: mutable.Queue[SonicMessage] = mutable.Queue(StreamStarted(ctx.traceId))
  var callType: CallType = ExecuteStatement
  val (topic, partition, filter) = parseQuery(query)

  /* BEHAVIOUR */

  def commonReceive: Receive = {
    case Status.Failure(e) ⇒ context.become(terminating(StreamCompleted.error(ctx.traceId, e)))
    case Completed ⇒ context.become(terminating(StreamCompleted.success(ctx.traceId)))
    case Cancel ⇒
      log.debug("client canceled")
      onComplete()
      context.stop(self)
  }

  def terminating(done: StreamCompleted): Receive = {
    tryPushDownstream()
    if (buffer.isEmpty && isActive && totalDemand > 0) {
      onNext(done)
      onCompleteThenStop()
    }

    {
      case r: Request ⇒ terminating(done)
    }
  }

  def materialized(upstream: ActorRef): Receive =
    commonReceive orElse {
      case Request(n) ⇒
        tryPushDownstream()
        if (totalDemand > 0 && pendingAck) {
          upstream ! Ack
          pendingAck = false
        }

      case c: ConsumerRecord[_, _] ⇒
        val record = c.asInstanceOf[ConsumerRecord[K, V]]

        if (filter(record)) {
          val message = OutputChunk(parseRecord(record).values.toVector)
          if (totalDemand > 0) {
            onNext(message)
          } else {
            buffer.enqueue(message)
          }
        }

        if (totalDemand > 0) {
          upstream ! Ack
        } else {
          pendingAck = true
        }
    }

  def waiting: Receive = commonReceive orElse {
    case Request(n) ⇒ tryPushDownstream()
    case Started ⇒
      buffer.enqueue(QueryProgress(QueryProgress.Started, 0, None, None))
      tryPushDownstream()
      context.become(materialized(sender()))

    case s: Source[_, _] ⇒
      s.asInstanceOf[Source[ConsumerRecord[K, V], _]].to(Sink.actorRefWithAck(self, Started, Ack, Completed))
  }

  override def receive: Receive = commonReceive orElse {
    case SubscriptionTimeoutExceeded ⇒
      log.info(s"no subscriber in within subs timeout $subscriptionTimeout")
      onCompleteThenStop()

    //first time client requests
    case Request(n) ⇒
      buffer.enqueue(QueryProgress(QueryProgress.Waiting, 0, None, None))
      supervisor ! GetBroadcastHub(topic, partition, settings)
      tryPushDownstream()
      context.become(waiting)
  }
}

object Kafka {

  val clazzLoader = this.getClass.getClassLoader

  def getSuperviorName(bootstrapServers: String, groupId: Option[String]): String = {
    bootstrapServers.split(",").sorted + groupId.map("_group_" + _).getOrElse("")
  }

  def getSupervisor(bootstrapServers: String, groupId: Option[String], actorContext: ActorContext): ActorRef = {
    val name = getSuperviorName(bootstrapServers, groupId)
    actorContext.child(name).getOrElse {
      actorContext.actorOf(Props(classOf[KafkaSupervisor],
        bootstrapServers, groupId, SonicdConfig.KAFKA_MAX_PARTITIONS, SonicdConfig.KAFKA_BROADCAST_BUFFER_SIZE), name)
    }
  }

  def getPublisherClass(settings: ConsumerSettings[_, _]): Class[_] = {
    classOf[KafkaPublisher[_, _]]
  }

  // json format to load from config
  implicit object DeserializerJsonFormat extends JsonFormat[Deserializer[String]] {
    override def write(obj: Deserializer[String]): JsValue = throw new Exception("not supported")

    override def read(json: JsValue): Deserializer[String] = json match {
      case JsString(des) ⇒
        Try(clazzLoader.loadClass(des))
          .getOrElse(clazzLoader.loadClass("org.apache.kafka.common.serialization." + des))
          .getConstructors()(0)
          .newInstance()
          .asInstanceOf[Deserializer[String]]
      case _ ⇒ throw new Exception("deserializer property expected to a be a string indicating either a Deserializer in 'org.apache.kafka.common.serialization' or the full class path custom one")
    }
  }

  case class GetBroadcastHub(topic: String, partition: Option[Int], settings: ConsumerSettings[_, _])

}
