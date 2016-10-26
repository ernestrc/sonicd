package build.unstable.sonicd.source

import akka.NotUsed
import akka.actor.{Actor, ActorContext, ActorRef, OneForOneStrategy, PoisonPill, Props, Status, SupervisorStrategy, Terminated}
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, KafkaConsumerActor, Subscriptions}
import akka.stream._
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request, SubscriptionTimeoutExceeded}
import akka.stream.scaladsl.{BroadcastHub, Keep, Sink, Source}
import build.unstable.sonic.JsonProtocol._
import build.unstable.sonic.model._
import build.unstable.sonicd.source.Kafka.GetBroadcastHub
import build.unstable.sonicd.source.SonicdSource.MissingConfigurationException
import build.unstable.sonicd.source.json.JsonUtils
import build.unstable.sonicd.{SonicdConfig, SonicdLogging}
import build.unstable.tylog
import build.unstable.tylog.{TypedLogging, Variation}
import com.typesafe.config._
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization._
import org.slf4j.MDC
import org.slf4j.event.Level
import spray.json._

import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.control.NonFatal
import scala.util.{Failure, Try}

class KafkaSource(query: Query, actorContext: ActorContext, context: RequestContext)
  extends SonicdSource(query, actorContext, context) with TypedLogging {

  import Kafka._

  // if a custom json format is specified, it must be fully specified concrete subclass of RootJsonFormat[S]
  // where S is the value deserialized by Kafka's Deserializer[S] specified in config
  // and must have a constructor that takes no a arguments
  def getJsonFormat(config: String): Option[RootJsonFormat[_]] = {
    getOption[Class[_]](config).map { clazz ⇒
      clazz.getConstructors()(0).newInstance().asInstanceOf[RootJsonFormat[_]]
    }
  }

  val settingsMap = getConfig[Map[String, JsValue]]("settings")
  val keyDeserializerClass = getConfig[String]("key-deserializer")
  val valueDeserializerClass = getConfig[String]("value-deserializer")
  val actorMaterializer = ActorMaterializer.create(actorContext)

  val settings = ConfigFactory.parseString(
    Map("kafka-clients" → settingsMap).toJson.compactPrint, ConfigParseOptions.defaults())
    .withFallback(Kafka.configDefaults)

  val keyDeserializer = Kafka.loadDeserializer(keyDeserializerClass)
  val valueDeserializer = Kafka.loadDeserializer(valueDeserializerClass)

  val keyJsonFormat = getJsonFormat(Kafka.keyJsonFormatConfig).orElse(Kafka.loadJsonFormat(keyDeserializer))
    .getOrElse(throw new Exception(s"could not infer json format from $keyDeserializerClass",
      new MissingConfigurationException(Kafka.keyJsonFormatConfig)))
  val valueJsonFormat = getJsonFormat(Kafka.valueJsonFormatConfig).orElse(Kafka.loadJsonFormat(valueDeserializer))
    .getOrElse(throw new Exception(s"could not infer json format from $valueDeserializerClass",
      new MissingConfigurationException(Kafka.valueJsonFormatConfig)))

  var consumerSettings = ConsumerSettings(settings, keyDeserializer, valueDeserializer)

  MDC.put(tylog.traceIdKey, context.traceId)
  log.info("using consummer settings {} with properties {}", consumerSettings, consumerSettings.properties)

  // by default is set to false, unless user overrides in query config
  val autoCommit = Option(consumerSettings.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG))
    .exists(_ == "true")

  val groupId = Option(consumerSettings.getProperty(ConsumerConfig.GROUP_ID_CONFIG))
  assert(!autoCommit || groupId.nonEmpty,
    """if auto commit is enabled, group.id must be set in the consumer settings: "group" : { "id" : "<ID>" }""")

  val bootstrapServers = Option(consumerSettings.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG))
    .getOrElse(throw new Exception(s"missing `${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG}` in `settings`"))

  override def publisher: Props = {
    val supervisor = getSupervisor(bootstrapServers, groupId, actorContext, actorMaterializer)
    Props(Kafka.getPublisherClass(consumerSettings), supervisor, query, consumerSettings,
      keyJsonFormat, valueJsonFormat, context, actorMaterializer)
  }
}

class KafkaSupervisor(bootstrapServers: String, groupId: Option[String], maxPartitions: Int, bufferSize: Int)
                     (implicit val materializer: ActorMaterializer) extends Actor with SonicdLogging {

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.debug("stopping kafka supervisor of {} for group {}", bootstrapServers, groupId)
  }

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    log.debug("starting kafka supervisor of {} for group {} with maxPartitions {} and bufferSize {}",
      bootstrapServers, groupId, maxPartitions, bufferSize)
  }

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case NonFatal(_) ⇒ SupervisorStrategy.Stop
  }

  val streams = mutable.Map.empty[String, (ActorRef, Source[_, NotUsed])]
  val subscribers = mutable.Map.empty[ActorRef, String]

  // FIXME: for now settings are ignored
  // so first query will set the consumer settings
  // and the rest will use the same subscriber
  // even if they have different settings
  // (except bootstrapServers, groupId)
  def getId(topic: String, partition: Option[Int], offset: Option[Long], settings: ConsumerSettings[_, _]): String =
  topic + partition.map(_.toString).getOrElse("") + offset.map(_.toString).getOrElse("") +
    settings.properties.values.toVector.sorted

  override def receive: Receive = {
    case Terminated(ref) ⇒
      // handled terminated sonic publisher
      subscribers.remove(ref).foreach { id ⇒
        // if there aren't any more subscribers kill graph
        if (subscribers.count(_._2 == id) == 0) {
          streams.remove(id).foreach { kv ⇒
            kv._1 ! PoisonPill
            log.info("shutting down down broadcast hub {}", id)
          }
        }
      }
      context unwatch ref

    case c@GetBroadcastHub(topic, partition, offset, settings) ⇒
      val id = getId(topic, partition, offset, settings)
      val sdr = sender()
      val message = streams.get(id).map(_._2).getOrElse {
        log.tylog(Level.INFO, c.ctx.traceId, MaterializeBroadcastHub(id), Variation.Attempt,
          "hub with id {} not found. materializing one..", id)

        lazy val actor = context.actorOf(KafkaConsumerActor.props(settings))

        ((partition, offset) match {
          // create manual subscription with given offset and partition
          case (Some(p), Some(o)) ⇒
            log.info("creating plain external source with settings {}", settings.properties)
            val subscription = Subscriptions.assignmentWithOffset(new TopicPartition(topic, p), o)
            Try(Consumer.plainExternalSource(actor, subscription))

          case (Some(p), None) ⇒
            log.info("creating plain external source with settings {}", settings.properties)
            val subscription = Subscriptions.assignment(new TopicPartition(topic, p))
            Try(Consumer.plainExternalSource(actor, subscription))

          // FIXME: disabled until we figure out how to terminate underlying resources
          // create auto subscription, offset will need to either be handled manually by client
          // or automatically if offset set to true in query config
          //  kafka-clients {
          //    enable.auto.commit = true
          //  }
          // case (None, None) ⇒
          //   log.debug("creating plain source with settings {}", settings.properties)
          //   Try(Consumer.plainSource(settings, Subscriptions.topics(topic)))
          //
          // error: both need to be set
          case _ ⇒
            Failure(new Exception("unable to create manual subscription: both 'partition' and 'offset' need to be set"))
        }).map { source ⇒

          val broadcast = source.toMat(BroadcastHub.sink(bufferSize))(Keep.right).run()

          // monitor subscriber termination to determine if graph should be terminated
          context watch sdr

          streams.update(id, actor → broadcast)
          subscribers.update(sdr, id)

          log.tylog(Level.INFO, c.ctx.traceId, MaterializeBroadcastHub(id), Variation.Success,
            "successfully materialized {} with consumer properties {}", id, settings.properties)
          broadcast: Source[_, _]
        }.recover {
          case e: Exception ⇒
            streams.remove(id) //just in case the error was in the flow or sink phases materialization
            log.tylog(Level.INFO, c.ctx.traceId, MaterializeBroadcastHub(id), Variation.Failure(e), "")
            Status.Failure(e)
        }.get
      }

      sdr ! message
  }
}

class KafkaPublisher[K, V](supervisor: ActorRef, query: Query, settings: ConsumerSettings[K, V],
                           kFormat: JsonFormat[K], vFormat: JsonFormat[V])
                          (implicit ctx: RequestContext, materializer: ActorMaterializer)
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

  override def unhandled(message: Any): Unit = {
    log.warning("{}({}) unexpected message: {}", self.path, this.getClass, message)
    super.unhandled(message)
  }

  /* HELPERS */

  def tryPushDownstream() {
    while (isActive && totalDemand > 0 && buffer.nonEmpty) {
      onNext(buffer.dequeue())
    }
  }

  // TODO should return select as well so that we can filter on that
  // like LocalJsonSource
  def parseQuery(query: Query): (String, Option[Int], Option[Long], ConsumerRecord[K, V] ⇒ Boolean) = {
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

    val offset: Option[Long] = obj.get("offset").map(p ⇒ Try(p.convertTo[Long])
      .getOrElse(throw new Exception("'offset' in query must be an integer")))

    // FIXME val filter = { record: ConsumerRecord[K,V] ⇒ parsed.valueFilter(parseRecord(record)) }

    (topic, partition, offset, { _ ⇒ true })
  }

  // TODO implement incremental type metadata
  def parseRecord(c: ConsumerRecord[K, V]): Map[String, JsValue] = {
    if (c.key() != null && c.value() != null) Map("key" → c.key.toJson(kFormat), "value" → c.value.toJson(vFormat))
    else if (c.value() != null) Map("value" → c.value().toJson(vFormat))
    else Map.empty
  }

  /* STATE */

  var bufferedMeta: Boolean = false
  var pendingAck: Boolean = false
  val buffer: mutable.Queue[SonicMessage] = mutable.Queue(StreamStarted(ctx.traceId))
  var callType: CallType = ExecuteStatement
  val (topic, partition, offset, filter) = parseQuery(query)

  /* BEHAVIOUR */

  def commonReceive: Receive = {
    case Status.Failure(e) ⇒ context.become(terminating(StreamCompleted.error(ctx.traceId, e)))
    case s: StreamCompleted ⇒ context.become(terminating(s))
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
        log.trace("Received record {}", c)
        val record = c.asInstanceOf[ConsumerRecord[K, V]]

        if (filter(record)) {
          val message = OutputChunk(parseRecord(record).values.toVector)
          if (totalDemand > 0) {
            onNext(message)
          } else {
            buffer.enqueue(message)
          }
        } else log.trace("filtered out record {}", record)

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
      log.debug("materialized kafka stream for {}", ctx.traceId)

      sender() ! Ack
      context.become(materialized(sender()))

    case s: Source[_, _] ⇒
      log.debug("received kafka stream source {} for {}", s, ctx.traceId)
      s.asInstanceOf[Source[ConsumerRecord[K, V], _]].to(Sink.actorRefWithAck(self, Started, Ack, Completed)).run()
  }

  override def receive: Receive = commonReceive orElse {
    case SubscriptionTimeoutExceeded ⇒
      log.info(s"no subscriber in within subs timeout $subscriptionTimeout")
      onCompleteThenStop()

    //first time client requests
    case Request(n) ⇒
      buffer.enqueue(QueryProgress(QueryProgress.Waiting, 0, None, None))
      supervisor ! GetBroadcastHub(topic, partition, offset, settings)
      tryPushDownstream()
      context.become(waiting)
  }
}

object Kafka extends SonicdLogging {

  val clazzLoader = this.getClass.getClassLoader
  val configDefaults = ConfigFactory.load().getObject("akka.kafka.consumer")

  log.info("loaded kafka config defaults: {}", configDefaults)

  def getSuperviorName(bootstrapServers: String, groupId: Option[String]): String = {
    bootstrapServers.split(",").sorted.reduce(_ + _) + groupId.map("_group_" + _).getOrElse("")
  }

  def getSupervisor(bootstrapServers: String, groupId: Option[String],
                    actorContext: ActorContext, materializer: ActorMaterializer): ActorRef = {
    val name = getSuperviorName(bootstrapServers, groupId)
    actorContext.child(name).getOrElse {
      actorContext.actorOf(Props(classOf[KafkaSupervisor],
        bootstrapServers, groupId, SonicdConfig.KAFKA_MAX_PARTITIONS,
        SonicdConfig.KAFKA_BROADCAST_BUFFER_SIZE, materializer
      ), name)
    }
  }

  def getPublisherClass(settings: ConsumerSettings[_, _]): Class[_] = {
    classOf[KafkaPublisher[_, _]]
  }

  def loadJsonFormat(s: Deserializer[_]): Option[JsonFormat[_]] = s match {
    case d: DoubleDeserializer ⇒ Some(DoubleJsonFormat)
    case d: IntegerDeserializer ⇒ Some(IntJsonFormat)
    case d: LongDeserializer ⇒ Some(LongJsonFormat)
    case d: StringDeserializer ⇒ Some(StringJsonFormat)
    // all the byte deserializers have been excluded to force
    // providing a higher level format
    case _ ⇒ None
  }

  def loadDeserializer(clazz: String): Deserializer[_] =
    Try(clazzLoader.loadClass(clazz))
      .getOrElse(clazzLoader.loadClass("org.apache.kafka.common.serialization." + clazz))
      .getConstructors()(0)
      .newInstance()
      .asInstanceOf[Deserializer[_]]

  case class GetBroadcastHub(topic: String, partition: Option[Int],
                             offset: Option[Long], settings: ConsumerSettings[_, _])(implicit val ctx: RequestContext)

  val keyJsonFormatConfig = "key-json-format"
  val valueJsonFormatConfig = "value-json-format"
}
