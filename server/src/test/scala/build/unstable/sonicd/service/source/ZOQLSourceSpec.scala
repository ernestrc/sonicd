package build.unstable.sonicd.service.source

// FIXME solve DI in Zuora Source and implement tests
/*
class ZOQLSourceSpec(_system: ActorSystem)
  extends TestKit(_system) with WordSpecLike
  with Matchers with BeforeAndAfterAll with ImplicitSender
  with ImplicitSubscriber {

  override protected def afterAll(): Unit =
    TestKit.shutdownActorSystem(system)

  def this() = this(ActorSystem("ZOQLSpec"))

  val config = """{ "class" : "ZuoraObjectQueryLanguageSource" }""".parseJson.asJsObject

  val controller: TestActorRef[TestController] =
    TestActorRef(Props[TestController].withDispatcher(CallingThreadDispatcher.Id))

  def newPublisher(query: String): TestActorRef[ZOQLPublisher] = {
    val src = new ZuoraObjectQueryLanguageSource(config, "test", query, controller.underlyingActor.context)
    val ref = TestActorRef[ZOQLPublisher](src.handlerProps.withDispatcher(CallingThreadDispatcher.Id))
    ActorPublisher(ref).subscribe(subs)
    ref
  }

  "ZuoraObjectQueryLanguageSource" should {
    "run a simple statement" in {
      false shouldBe true
    }

    "close connection after running one statement" in {
      false shouldBe true
    }

    "send typed values downstream" in {
      false shouldBe true
    }

    "extract fields from query" in {
      val q = "select a,b,c from usage;"
      val q2 = "SELECT   fish ,  b, C     FROM usage where nonesense is true;"
      ZuoraObjectQueryLanguageSource
        .extractSelectColumnNames(q) should contain theSameElementsAs Seq("a", "b", "c")
      ZuoraObjectQueryLanguageSource
        .extractSelectColumnNames(q2) should contain theSameElementsAs Seq("fish", "b", "C")
    }

    "should send type metadata" in {
      false shouldBe true
    }
  }
}
  */
