package build.unstable.sonicd.source

import akka.actor.{ActorContext, ActorRef, Props}
import akka.kafka.ConsumerSettings
import akka.stream._
import build.unstable.sonic.JsonProtocol._
import build.unstable.sonic.model._
import build.unstable.sonicd.source.SonicdSource.MissingConfigurationException
import build.unstable.sonicd.source.kafka.{KafkaPublisher, KafkaSupervisor}
import build.unstable.sonicd.{SonicdConfig, SonicdLogging}
import build.unstable.tylog
import build.unstable.tylog.TypedLogging
import com.typesafe.config._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization._
import org.slf4j.MDC
import spray.json._

import scala.util.Try

class KafkaSource(query: Query, actorContext: ActorContext, context: RequestContext)
  extends SonicdSource(query, actorContext, context) with TypedLogging {

  // if a custom json format is specified, it must be fully specified concrete subclass of RootJsonFormat[S]
  // where S is the value deserialized by Kafka's Deserializer[S] specified in config
  // and must have a constructor that takes no a arguments
  def getJsonFormat(config: String): Option[RootJsonFormat[_]] = {
    getOption[Class[_]](config).map { clazz ⇒
      clazz.getConstructors()(0).newInstance().asInstanceOf[RootJsonFormat[_]]
    }
  }

  val ignoreParsingErrors = getOption[Int]("ignore-parsing-errors")
  val settingsMap = getConfig[Map[String, JsValue]]("settings")
  val keyDeserializerClass = getConfig[String]("key-deserializer")
  val valueDeserializerClass = getConfig[String]("value-deserializer")
  val actorMaterializer = ActorMaterializer.create(actorContext)

  val settings = ConfigFactory.parseString(
    Map("kafka-clients" → settingsMap).toJson.compactPrint, ConfigParseOptions.defaults())
    .withFallback(KafkaSource.configDefaults)

  val keyDeserializer = KafkaSource.loadDeserializer(keyDeserializerClass)
  val valueDeserializer = KafkaSource.loadDeserializer(valueDeserializerClass)

  val keyJsonFormat = getJsonFormat(KafkaSource.keyJsonFormatConfig).orElse(KafkaSource.loadJsonFormat(keyDeserializer))
    .getOrElse(throw new Exception(s"could not infer json format from $keyDeserializerClass",
      new MissingConfigurationException(KafkaSource.keyJsonFormatConfig)))
  val valueJsonFormat = getJsonFormat(KafkaSource.valueJsonFormatConfig).orElse(KafkaSource.loadJsonFormat(valueDeserializer))
    .getOrElse(throw new Exception(s"could not infer json format from $valueDeserializerClass",
      new MissingConfigurationException(KafkaSource.valueJsonFormatConfig)))

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
    val supervisor = KafkaSource.getSupervisor(bootstrapServers, groupId, actorContext, actorMaterializer)
    Props(KafkaSource.getPublisherClass(consumerSettings), supervisor, query, consumerSettings,
      keyJsonFormat, valueJsonFormat, ignoreParsingErrors, context, actorMaterializer)
  }
}

object KafkaSource extends SonicdLogging {

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
    // providing a customized json format
    case _ ⇒ None
  }

  def loadDeserializer(clazz: String): Deserializer[_] =
    Try(clazzLoader.loadClass(clazz))
      .getOrElse(clazzLoader.loadClass("org.apache.kafka.common.serialization." + clazz))
      .getConstructors()(0)
      .newInstance()
      .asInstanceOf[Deserializer[_]]

  val keyJsonFormatConfig = "key-json-format"
  val valueJsonFormatConfig = "value-json-format"

  // RootJsonFormat to use with kafka's StringDeserializer on
  // messages that can be parsed as JSON
  class JSONParser extends RootJsonFormat[String] {
    override def read(json: JsValue): String = {
      json match {
        case JsString(s) ⇒ s
        case j ⇒ j.compactPrint
      }
    }

    override def write(obj: String): JsValue = obj.parseJson
  }
}
