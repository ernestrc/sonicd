package build.unstable.sonicd.service.source

import akka.actor._
import akka.stream.actor.ActorPublisherMessage.Cancel
import akka.stream.actor.{ActorPublisher, ActorPublisherMessage}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.testkit.{CallingThreadDispatcher, ImplicitSender, TestActorRef, TestKit}
import build.unstable.sonic.model.{OutputChunk, Query, QueryProgress, RequestContext}
import build.unstable.sonicd.model._
import build.unstable.sonicd.source.kafka.{KafkaPublisher, KafkaSupervisor}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import spray.json._
import build.unstable.sonic.JsonProtocol._
import build.unstable.sonicd.source.KafkaSource

import scala.collection.mutable

class KafkaSourceSpec(_system: ActorSystem)
  extends TestKit(_system) with WordSpecLike
    with Matchers with BeforeAndAfterAll with ImplicitSender
    with ImplicitSubscriber with HandlerUtils {

  import Fixture._

  override protected def afterAll(): Unit = {
    materializer.shutdown()
    TestKit.shutdownActorSystem(system)
  }

  def this() = this(ActorSystem("KafkaSourceSpec"))

  implicit val ctx: RequestContext = RequestContext("test_trace_id", None)

  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system))

  val defaultConsumerSettings: Map[String, JsValue] = {
    Map(
      "bootstrap" → JsObject(Map(
        "servers" → JsString("localhost:9092")
      ))
    )
  }

  val controller: TestActorRef[TestController] =
    TestActorRef(Props(classOf[TestController], self).withDispatcher(CallingThreadDispatcher.Id))

  def newPublisher[K, V](q: String,
                         consumerSettings: Map[String, JsValue],
                         context: RequestContext = testCtx,
                         keyDeserializer: String = "StringDeserializer",
                         valueDeserializer: String = "StringDeserializer",
                         keyJsonFormat: Option[JsonFormat[K]] = None,
                         valueJsonFormat: Option[JsonFormat[V]] = None,
                         ignoreParsingErrors: Option[Int] = None,
                         dispatcher: String = CallingThreadDispatcher.Id): TestActorRef[KafkaPublisher[K, V]] = {

    val fields = mutable.Map(
      "class" → JsString("KafkaSource"),
      "key-deserializer" → JsString(keyDeserializer),
      "value-deserializer" → JsString(valueDeserializer),
      "settings" → JsObject(consumerSettings)
    )

    keyJsonFormat.foreach(k ⇒ fields.update("key-json-format", JsString(k.getClass.getName)))
    valueJsonFormat.foreach(k ⇒ fields.update("value-json-format", JsString(k.getClass.getName)))

    val config = JsObject(fields.toMap)
    val query = new Query(Some(1L), Some("traceId"), None, q, config)
    val src = new TestKafkaSource(self, query, controller.underlyingActor.context, context)
    val ref = TestActorRef[KafkaPublisher[K, V]](src.publisher.withDispatcher(dispatcher))
    ActorPublisher(ref).subscribe(subs)
    watch(ref)
    ref
  }

  def expectGetBroadcastHub(topic: String,
                            offset: Option[Long] = None,
                            partition: Option[Int] = None): KafkaSupervisor.GetBroadcastHub = {
    val cmd = expectMsgType[KafkaSupervisor.GetBroadcastHub]
    cmd.offset shouldBe offset
    cmd.partition shouldBe partition
    cmd.topic shouldBe topic
    cmd
  }

  def buildQuery(topic: String,
                 offset: Option[Long] = None,
                 partition: Option[Long] = None,
                 filter: Vector[(String, String)] = Vector.empty,
                 select: Vector[String] = Vector.empty): String = {
    val fields = mutable.Map(
      "topic" → topic.toJson,
      "offset" → offset.toJson,
      "partition" → partition.toJson
    )

    if (filter.nonEmpty) {
      fields.update("filter", filter.toMap.toJson)
    }

    if (select.nonEmpty) {
      fields.update("select", select.toJson)
    }

    JsObject(fields.toMap).compactPrint
  }

  "KafkaSource" should {
    "run a simple query and stream [String,String] consumer records" in {
      val topic = "topic1"
      val query = buildQuery(topic)
      val pub = newPublisher[String, String](query, defaultConsumerSettings)

      pub ! ActorPublisherMessage.Request(2)
      expectGetBroadcastHub(topic)
      expectStreamStarted()
      expectQueryProgress(0, QueryProgress.Waiting, None, None)

      val proxy = newProxyPublisher[ConsumerRecord[String, String]]
      val source = newProxySource[ConsumerRecord[String, String]](proxy)
      pub ! source

      proxy ! new ConsumerRecord(topic, 0, 0, "B", "b")

      pub ! ActorPublisherMessage.Request(1)
      val meta = expectTypeMetadata()
      meta.typesHint shouldBe Vector(("B", JsString.empty))

      pub ! ActorPublisherMessage.Request(1)
      expectMsg(OutputChunk(Vector("b")))

      // new schema
      proxy ! new ConsumerRecord(topic, 0, 0, "G", "g")

      pub ! ActorPublisherMessage.Request(1)
      val meta2 = expectTypeMetadata()
      meta2.typesHint shouldBe Vector(("B", JsString.empty), ("G", JsString.empty))

      pub ! ActorPublisherMessage.Request(1)
      expectMsg(OutputChunk(JsArray(Vector(JsNull, JsString("g")))))

      pub ! Cancel
      expectTerminated(pub)
    }

    "run a simple query and stream [Number, Number] consumer records" in {
      val topic = "topic1"
      val query = buildQuery(topic)
      val pub = newPublisher[Long, Double](query, defaultConsumerSettings,
        keyDeserializer = "LongDeserializer",
        valueDeserializer = "DoubleDeserializer"
      )

      pub ! ActorPublisherMessage.Request(2)
      expectGetBroadcastHub(topic)
      expectStreamStarted()
      expectQueryProgress(0, QueryProgress.Waiting, None, None)

      val proxy = newProxyPublisher[ConsumerRecord[Long, Double]]
      val source = newProxySource[ConsumerRecord[Long, Double]](proxy)
      pub ! source

      proxy ! new ConsumerRecord(topic, 0, 0, 1L, 0.11d)

      pub ! ActorPublisherMessage.Request(1)
      val meta = expectTypeMetadata()
      meta.typesHint shouldBe Vector(("1", JsNumber.zero))

      pub ! ActorPublisherMessage.Request(1)
      expectMsg(OutputChunk(Vector(0.11d)))

      // new schema
      proxy ! new ConsumerRecord(topic, 0, 0, 2L, 0.22d)

      pub ! ActorPublisherMessage.Request(1)
      val meta2 = expectTypeMetadata()
      meta2.typesHint shouldBe Vector(("1", JsNumber.zero), ("2", JsNumber.zero))

      pub ! ActorPublisherMessage.Request(1)
      expectMsg(OutputChunk(JsArray(Vector(JsNull, JsNumber(0.22d)))))

      pub ! Cancel
      expectTerminated(pub)
    }

    "run a simple query with filters" in {
      val topic = "topic1"
      val query = buildQuery(topic, filter = Vector(("B", "b")), partition = Some(10))
      val pub = newPublisher[String, String](query, defaultConsumerSettings)

      pub ! ActorPublisherMessage.Request(2)
      expectGetBroadcastHub(topic, partition = Some(10))
      expectStreamStarted()
      expectQueryProgress(0, QueryProgress.Waiting, None, None)

      val proxy = newProxyPublisher[ConsumerRecord[String, String]]
      val source = newProxySource[ConsumerRecord[String, String]](proxy)
      pub ! source

      proxy ! new ConsumerRecord(topic, 0, 0, "B", "b")

      pub ! ActorPublisherMessage.Request(1)
      val meta = expectTypeMetadata()
      meta.typesHint shouldBe Vector(("B", JsString.empty))

      pub ! ActorPublisherMessage.Request(1)
      expectMsg(OutputChunk(Vector("b")))

      // should be filtered out
      proxy ! new ConsumerRecord(topic, 0, 0, "G", "g")

      pub ! ActorPublisherMessage.Request(1)

      pub ! Cancel
      expectTerminated(pub)
    }

    "run a simple query with select" in {
      val topic = "topic1"
      val query = buildQuery(topic, select = Vector("B"), offset = Some(10))
      val pub = newPublisher[String, String](query, defaultConsumerSettings)

      pub ! ActorPublisherMessage.Request(2)
      expectGetBroadcastHub(topic, offset = Some(10))
      expectStreamStarted()
      expectQueryProgress(0, QueryProgress.Waiting, None, None)

      val proxy = newProxyPublisher[ConsumerRecord[String, String]]
      val source = newProxySource[ConsumerRecord[String, String]](proxy)
      pub ! source

      proxy ! new ConsumerRecord(topic, 0, 0, "B", "b")

      pub ! ActorPublisherMessage.Request(1)
      val meta = expectTypeMetadata()
      meta.typesHint shouldBe Vector(("B", JsString.empty))

      pub ! ActorPublisherMessage.Request(1)
      expectMsg(OutputChunk(Vector("b")))

      // should be filtered out
      proxy ! new ConsumerRecord(topic, 0, 0, "G", "g")

      pub ! ActorPublisherMessage.Request(1)

      proxy ! new ConsumerRecord(topic, 0, 0, "B", "bbbb")

      pub ! ActorPublisherMessage.Request(1)
      expectMsg(OutputChunk(Vector("bbbb")))

      pub ! Cancel
      expectTerminated(pub)
    }

    "run a query with custom json format" in {
      val topic = "topic1"
      val query = buildQuery(topic)
      val pub = newPublisher[String, String](query, defaultConsumerSettings,
        keyDeserializer = "StringDeserializer",
        valueDeserializer = "StringDeserializer",
        valueJsonFormat = Some(new KafkaSource.JSONParser())
      )

      pub ! ActorPublisherMessage.Request(2)
      expectGetBroadcastHub(topic)
      expectStreamStarted()
      expectQueryProgress(0, QueryProgress.Waiting, None, None)

      val proxy = newProxyPublisher[ConsumerRecord[String, String]]
      val source = newProxySource[ConsumerRecord[String, String]](proxy)
      pub ! source

      proxy ! new ConsumerRecord(topic, 0, 0, "irrelevant", """{"B":"b"}""")

      pub ! ActorPublisherMessage.Request(1)
      val meta = expectTypeMetadata()
      meta.typesHint shouldBe Vector(("B", JsString.empty))

      pub ! ActorPublisherMessage.Request(1)
      expectMsg(OutputChunk(Vector("b")))

      // new schema
      proxy ! new ConsumerRecord(topic, 0, 0, "blabla", """{"G":"g"}""")

      pub ! ActorPublisherMessage.Request(1)
      val meta2 = expectTypeMetadata()
      meta2.typesHint shouldBe Vector(("B", JsString.empty), ("G", JsString.empty))

      pub ! ActorPublisherMessage.Request(1)
      expectMsg(OutputChunk(JsArray(Vector(JsNull, JsString("g")))))

      pub ! Cancel
      expectTerminated(pub)
    }

    "run a query with custom json format with filters" in {
      val topic = "topic1"
      val query = buildQuery(topic, filter = Vector(("B", "b")), partition = Some(10))
      val pub = newPublisher[String, String](query, defaultConsumerSettings,
        keyDeserializer = "StringDeserializer",
        valueDeserializer = "StringDeserializer",
        valueJsonFormat = Some(new KafkaSource.JSONParser())
      )

      pub ! ActorPublisherMessage.Request(2)
      expectGetBroadcastHub(topic, partition = Some(10))
      expectStreamStarted()
      expectQueryProgress(0, QueryProgress.Waiting, None, None)

      val proxy = newProxyPublisher[ConsumerRecord[String, String]]
      val source = newProxySource[ConsumerRecord[String, String]](proxy)
      pub ! source

      proxy ! new ConsumerRecord(topic, 0, 0, "irrelevant", """{"B":"b"}""")

      pub ! ActorPublisherMessage.Request(1)
      val meta = expectTypeMetadata()
      meta.typesHint shouldBe Vector(("B", JsString.empty))

      pub ! ActorPublisherMessage.Request(1)
      expectMsg(OutputChunk(Vector("b")))

      // should be filtered out
      proxy ! new ConsumerRecord(topic, 0, 0, "blabla", """{"G":"g"}""")

      pub ! ActorPublisherMessage.Request(1)

      pub ! Cancel
      expectTerminated(pub)
    }

    "run a query with custom json format with select" in {
      val topic = "topic1"
      val query = buildQuery(topic, select = Vector("B"), offset = Some(10))
      val pub = newPublisher[String, String](query, defaultConsumerSettings,
        keyDeserializer = "StringDeserializer",
        valueDeserializer = "StringDeserializer",
        valueJsonFormat = Some(new KafkaSource.JSONParser())
      )

      pub ! ActorPublisherMessage.Request(2)
      expectGetBroadcastHub(topic, offset = Some(10))
      expectStreamStarted()
      expectQueryProgress(0, QueryProgress.Waiting, None, None)

      val proxy = newProxyPublisher[ConsumerRecord[String, String]]
      val source = newProxySource[ConsumerRecord[String, String]](proxy)
      pub ! source

      proxy ! new ConsumerRecord(topic, 0, 0, "irrelevant", """{"B":"b"}""")

      pub ! ActorPublisherMessage.Request(1)
      val meta = expectTypeMetadata()
      meta.typesHint shouldBe Vector(("B", JsString.empty))

      pub ! ActorPublisherMessage.Request(1)
      expectMsg(OutputChunk(Vector("b")))

      // should be filtered out
      proxy ! new ConsumerRecord(topic, 0, 0, "blabla", """{"G":"g"}""")

      pub ! ActorPublisherMessage.Request(1)

      proxy ! new ConsumerRecord(topic, 0, 0, "irrelevant", """{"B":"bbbb"}""")

      pub ! ActorPublisherMessage.Request(1)
      expectMsg(OutputChunk(Vector("bbbb")))

      pub ! Cancel
      expectTerminated(pub)
    }

    "reject a query without missing (required) configuration parameters" in {
      // missing bootstrap servers
      assert(intercept[Exception] {
        newPublisher[String, String]("""{"topic": "blabla"}""", Map.empty)
      }.getMessage.toLowerCase.contains("missing"))

      // custom deserializer or complex type deserializer requires a custom json format
      assert(intercept[Exception] {
        newPublisher[String, String](
          """{"topic": "blabla"}""", defaultConsumerSettings,
          keyDeserializer = "ByteArrayDeserializer",
          valueDeserializer = "ByteArrayDeserializer",
          valueJsonFormat = None
        )
      }.getMessage.toLowerCase.contains("json format"))
      assert(intercept[Exception] {
        val pub = newPublisher[String, String](
          """{"topic": "blabla"}""", defaultConsumerSettings,
          keyDeserializer = "ByteBufferDeserializer",
          valueDeserializer = "ByteBufferDeserializer",
          valueJsonFormat = None
        )
      }.getMessage.toLowerCase.contains("json format"))
      assert(intercept[Exception] {
        newPublisher[String, String](
          """{"topic": "blabla"}""", defaultConsumerSettings,
          keyDeserializer = "BytesDeserializer",
          valueDeserializer = "BytesDeserializer",
          valueJsonFormat = None
        )
      }.getMessage.toLowerCase.contains("json format"))

      // deserializer not found in class path
      intercept[Exception] {
        newPublisher[String, String](
          """{"topic": "blabla"}""", defaultConsumerSettings,
          keyDeserializer = "jklajkla",
          valueDeserializer = "jlkdsjklf",
          valueJsonFormat = None
        )
      }
      // if autoCommit is set then groupId must also be set
      assert(intercept[Exception] {
        val settings =
          Map(
            "enable" → JsObject(Map(
              "auto" → JsObject(Map(
                "commit" → JsBoolean(true)
              ))
            )),
            "bootstrap" → JsObject(Map(
              "servers" → JsString("localhost:9092")
            ))
          )
        newPublisher[String, String]("""{"topic": "blabla"}""", settings)
      }.getMessage.contains("group.id"))

    }
  }
}

//override supervisor
class TestKafkaSource[K, V](implicitSender: ActorRef, query: Query,
                            actorContext: ActorContext, context: RequestContext)
  extends build.unstable.sonicd.source.KafkaSource(query, actorContext, context) {

  override def getSupervisor(bootstrapServers: String, groupId: Option[String],
                             actorContext: ActorContext,
                             materializer: ActorMaterializer): ActorRef = implicitSender
}
