package build.unstable.sonicd.service.source

import build.unstable.sonic.model.{OutputChunk, SonicMessage, TypeMetadata}
import build.unstable.sonicd.source.SonicdPublisher
import build.unstable.sonicd.source.SonicdPublisher.ParsedQuery
import org.scalatest.{Matchers, WordSpec}
import spray.json.{JsArray, JsBoolean, JsNull, JsNumber, JsObject, JsString, JsValue}

import scala.collection.immutable.Map
import scala.collection.mutable

class SonicdPublisherSpec extends WordSpec with Matchers {

  def newCase() = new SonicdPublisher {
    override val buffer: mutable.Queue[SonicMessage] = mutable.Queue.empty[SonicMessage]
  }

  val noFilter = (j: JsObject) ⇒ true
  val noSelect = None

  "SonicdPublisher with no filters" should {
    "ignore data when not a JsObject and select is defined" in {
      val test = newCase()
      val query = ParsedQuery(Some(Vector("a", "b")), noFilter)

      test.bufferNext(query, JsString("1234"))
      test.bufferNext(query, JsNumber(1))
      test.bufferNext(query, JsNull)
      test.bufferNext(query, JsBoolean(true))
      test.bufferNext(query, JsArray(JsNumber(1)))

      test.buffer.isEmpty shouldBe true //shouldBe mutable.Queue.empty[SonicMessage]
    }

    "if data is not a JsObject should encode it as 'raw' -> value" in {
      val test = newCase()
      val query = ParsedQuery(noSelect, noFilter)

      test.bufferNext(query, JsString("1234"))
      test.buffer.dequeue()
        .asInstanceOf[TypeMetadata].typesHint should contain theSameElementsInOrderAs Vector(("raw", JsString.empty))
      test.buffer.dequeue()
        .asInstanceOf[OutputChunk].data.elements should contain theSameElementsInOrderAs Vector(JsString("1234"))

      test.bufferNext(query, JsNumber(1))
      test.buffer.dequeue()
        .asInstanceOf[TypeMetadata].typesHint should contain theSameElementsInOrderAs Vector(("raw", JsNumber.zero))
      test.buffer.dequeue()
        .asInstanceOf[OutputChunk].data.elements should contain theSameElementsInOrderAs Vector(JsNumber(1))

      test.bufferNext(query, JsNull)
      test.buffer.dequeue()
        .asInstanceOf[TypeMetadata].typesHint should contain theSameElementsInOrderAs Vector(("raw", JsNull))
      test.buffer.dequeue()
        .asInstanceOf[OutputChunk].data.elements should contain theSameElementsInOrderAs Vector(JsNull)

      test.bufferNext(query, JsBoolean(false))
      test.buffer.dequeue()
        .asInstanceOf[TypeMetadata].typesHint should contain theSameElementsInOrderAs Vector(("raw", JsBoolean(true)))
      test.buffer.dequeue()
        .asInstanceOf[OutputChunk].data.elements should contain theSameElementsInOrderAs Vector(JsBoolean(false))

      test.bufferNext(query, JsArray(JsNumber(1)))
      test.buffer.dequeue()
        .asInstanceOf[TypeMetadata].typesHint should contain theSameElementsInOrderAs Vector(("raw", JsArray.empty))
      test.buffer.dequeue()
        .asInstanceOf[OutputChunk].data.elements should contain theSameElementsInOrderAs Vector(JsArray(JsNumber(1)))

      test.buffer.isEmpty shouldBe true
    }

    "incrementally emit new type metadata that conform to the previously seen values" in {
      val test = newCase()
      val query = ParsedQuery(noSelect, noFilter)

      test.bufferNext(query, JsString("hello"))
      test.buffer.dequeue()
        .asInstanceOf[TypeMetadata].typesHint should contain theSameElementsInOrderAs Vector(("raw", JsString.empty))
      test.buffer.dequeue()
        .asInstanceOf[OutputChunk].data.elements should contain theSameElementsInOrderAs Vector(JsString("hello"))

      test.bufferNext(query, JsString("hello2"))
      test.buffer.dequeue()
        .asInstanceOf[OutputChunk].data.elements should contain theSameElementsInOrderAs Vector(JsString("hello2"))

      test.bufferNext(query, JsObject(Map("a" → JsString("1234"), "b" → JsNumber(1))))
      test.buffer.dequeue()
        .asInstanceOf[TypeMetadata].typesHint should contain
        .theSameElementsInOrderAs(Vector(("raw", JsString.empty), ("a", JsString.empty), ("b", JsNumber.zero)))
      test.buffer.dequeue()
        .asInstanceOf[OutputChunk].data.elements should contain
        .theSameElementsInOrderAs(Vector(JsNull, JsString("1234"), JsNumber(1)))

      // no new field, so no new metadata
      test.bufferNext(query, JsObject(Map("a" → JsString("4321"), "b" → JsNumber(-1))))
      test.buffer.dequeue()
        .asInstanceOf[OutputChunk].data.elements should contain
        .theSameElementsInOrderAs(Vector(JsNull, JsString("4321"), JsNumber(-1)))

      // no new field, but new types
      test.bufferNext(query, JsObject(Map("a" → JsString("4321"), "b" → JsBoolean(false))))
      test.buffer.dequeue()
        .asInstanceOf[TypeMetadata].typesHint should contain
        .theSameElementsInOrderAs(Vector(("raw", JsString.empty), ("a", JsString.empty), ("b", JsBoolean(true))))

      test.buffer.dequeue()
        .asInstanceOf[OutputChunk].data.elements should contain
        .theSameElementsInOrderAs(Vector(JsNull, JsString("4321"), JsBoolean(false)))

      test.buffer.isEmpty shouldBe true
    }

    "incrementally emit new type metadata that conform to the select, if types change" in {
      val test = newCase()
      val query = ParsedQuery(Some(Vector("a", "b")), noFilter)

      test.bufferNext(query, JsString("hello"))

      test.bufferNext(query, JsObject(Map("a" → JsString("1234"), "b" → JsNumber(1), "c" → JsBoolean(true))))
      test.buffer.dequeue()
        .asInstanceOf[TypeMetadata].typesHint should contain
        .theSameElementsInOrderAs(Vector(("a", JsString.empty), ("b", JsNumber.zero)))
      test.buffer.dequeue()
        .asInstanceOf[OutputChunk].data.elements should contain
        .theSameElementsInOrderAs(Vector(JsString("1234"), JsNumber(1)))

      // no new field, so no new metadata
      test.bufferNext(query, JsObject(Map("a" → JsString("4321"), "b" → JsNumber(-1))))
      test.buffer.dequeue()
        .asInstanceOf[OutputChunk].data.elements should contain
        .theSameElementsInOrderAs(Vector(JsString("4321"), JsNumber(-1)))

      // no new field, but new types
      test.bufferNext(query, JsObject(Map("a" → JsString("4321"), "b" → JsBoolean(false))))
      test.buffer.dequeue()
        .asInstanceOf[TypeMetadata].typesHint should contain
        .theSameElementsInOrderAs(Vector(("a", JsString.empty), ("b", JsBoolean(true))))
      test.buffer.dequeue()
        .asInstanceOf[OutputChunk].data.elements should contain
        .theSameElementsInOrderAs(Vector(JsString("4321"), JsBoolean(false)))

      test.buffer.isEmpty shouldBe true
    }
  }

  "SonicdPublisher with filters" should {
    "ignore data when not a JsObject and select is defined" in {
      val test = newCase()
      val filter: PartialFunction[JsValue, Boolean] = {
        case j@JsObject(f) ⇒ f.exists(kv => kv._2 == JsString("1234"))
        case _ ⇒ false
      }

      val query = ParsedQuery(Some(Vector("a", "b")), filter)

      test.bufferNext(query, JsString("1234"))
      test.bufferNext(query, JsNumber(1))
      test.bufferNext(query, JsString("somethingElse"))
      test.bufferNext(query, JsNull)
      test.bufferNext(query, JsBoolean(true))
      test.bufferNext(query, JsArray(JsNumber(1)))

      test.buffer.isEmpty shouldBe true
    }

    "if data is not a JsObject should encode it as 'raw' -> value" in {
      val test = newCase()
      val filter: PartialFunction[JsObject, Boolean] = {
        case j@JsObject(fields) ⇒ fields == Map("raw" → JsString("1234"))
      }
      val query = ParsedQuery(noSelect, filter)

      test.bufferNext(query, JsString("1234"))
      test.buffer.dequeue()
        .asInstanceOf[TypeMetadata].typesHint should contain theSameElementsInOrderAs Vector(("raw", JsString.empty))
      test.buffer.dequeue()
        .asInstanceOf[OutputChunk].data.elements should contain theSameElementsInOrderAs Vector(JsString("1234"))

      test.bufferNext(query, JsNumber(1))
      test.bufferNext(query, JsNull)
      test.bufferNext(query, JsBoolean(false))
      test.bufferNext(query, JsArray(JsNumber(1)))
      test.bufferNext(query, JsString("1235"))

      test.buffer.isEmpty shouldBe true
    }

    "incrementally emit new type metadata that conform to the previously seen values" in {
      val test = newCase()
      val filter: PartialFunction[JsObject, Boolean] = {
        case j@JsObject(f) ⇒ f.exists(kv => kv._1 == "a" && kv._2 == JsString("1234"))
      }
      val query = ParsedQuery(noSelect, filter)

      test.bufferNext(query, JsString("hello"))
      test.bufferNext(query, JsString("hello"))
      test.buffer.isEmpty shouldBe true

      test.bufferNext(query, JsObject(Map("a" → JsString("1235"), "b" → JsNumber(1))))
      test.buffer.isEmpty shouldBe true

      test.bufferNext(query, JsObject(Map("a" → JsString("1234"), "b" → JsNumber(1))))
      test.buffer.dequeue()
        .asInstanceOf[TypeMetadata].typesHint should contain
        .theSameElementsInOrderAs(Vector(("a", JsString.empty), ("b", JsNumber.zero)))

      test.buffer.dequeue()
        .asInstanceOf[OutputChunk].data.elements should contain
        .theSameElementsInOrderAs(Vector(JsString("1234"), JsNumber(1)))

      // new field but doesn't pass filter
      test.bufferNext(query, JsObject(Map("c" → JsNumber(10), "a" → JsString("wjlkrjweklr"), "b" → JsNumber(1))))
      test.buffer.isEmpty shouldBe true

      test.bufferNext(query, JsObject(Map("c" → JsNumber(10), "a" → JsString("1234"), "b" → JsNumber(1))))
      test.buffer.dequeue()
        .asInstanceOf[TypeMetadata].typesHint should contain
        .theSameElementsInOrderAs(Vector(("a", JsString.empty), ("b", JsNumber.zero), ("c", JsNumber.zero)))

      test.buffer.dequeue()
        .asInstanceOf[OutputChunk].data.elements should contain
        .theSameElementsInOrderAs(Vector(JsString("1234"), JsNumber(1), JsNumber(10)))

      // no new field, so no new metadata
      test.bufferNext(query, JsObject(Map("a" → JsString("1234"), "b" → JsNumber(-1))))
      test.buffer.dequeue()
        .asInstanceOf[OutputChunk].data.elements should contain
        .theSameElementsInOrderAs(Vector(JsString("1234"), JsNumber(-1), JsNull))

      // no new field, but new types
      test.bufferNext(query, JsObject(Map("a" → JsString("1234"), "b" → JsBoolean(false))))
      test.buffer.dequeue()
        .asInstanceOf[TypeMetadata].typesHint should contain
        .theSameElementsInOrderAs(Vector(("a", JsString.empty), ("b", JsBoolean(true)), ("c", JsNumber.zero)))

      test.buffer.dequeue()
        .asInstanceOf[OutputChunk].data.elements should contain
        .theSameElementsInOrderAs(Vector(JsString("1234"), JsBoolean(false), JsNull))

      test.buffer.isEmpty shouldBe true
    }

    "incrementally emit new type metadata that conform to the select, if types change" in {
      val test = newCase()
      val filter: PartialFunction[JsValue, Boolean] = {
        case j@JsObject(f) ⇒ f.exists(kv => kv._1 == "a" && kv._2 == JsString("1234"))
        case _ ⇒ false
      }
      val query = ParsedQuery(Some(Vector("a", "b")), filter)

      test.bufferNext(query, JsString("hello"))

      test.bufferNext(query, JsObject(Map("a" → JsString("1234"), "b" → JsNumber(1), "c" → JsBoolean(true))))
      test.buffer.dequeue()
        .asInstanceOf[TypeMetadata].typesHint should contain
        .theSameElementsInOrderAs(Vector(("a", JsString.empty), ("b", JsNumber.zero)))
      test.buffer.dequeue()
        .asInstanceOf[OutputChunk].data.elements should contain
        .theSameElementsInOrderAs(Vector(JsString("1234"), JsNumber(1)))

      // no new field, so no new metadata
      test.bufferNext(query, JsObject(Map("a" → JsString("1234"), "b" → JsNumber(-1))))
      test.buffer.dequeue()
        .asInstanceOf[OutputChunk].data.elements should contain
        .theSameElementsInOrderAs(Vector(JsString("1234"), JsNumber(-1)))

      // no new field, but new types
      test.bufferNext(query, JsObject(Map("a" → JsString("1234"), "b" → JsBoolean(false))))
      test.buffer.dequeue()
        .asInstanceOf[TypeMetadata].typesHint should contain
        .theSameElementsInOrderAs(Vector(("a", JsString.empty), ("b", JsBoolean(true))))
      test.buffer.dequeue()
        .asInstanceOf[OutputChunk].data.elements should contain
        .theSameElementsInOrderAs(Vector(JsString("1234"), JsBoolean(false)))

      test.buffer.isEmpty shouldBe true
    }
  }
}
