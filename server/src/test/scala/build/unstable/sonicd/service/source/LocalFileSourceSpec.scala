package build.unstable.sonicd.service.source

import java.io.{File, FileOutputStream}
import java.nio.charset.Charset
import java.nio.file._

import akka.actor._
import akka.stream.actor.ActorPublisherMessage.Cancel
import akka.stream.actor.{ActorPublisher, ActorPublisherMessage}
import akka.testkit.{CallingThreadDispatcher, ImplicitSender, TestActorRef, TestKit}
import build.unstable.sonic.JsonProtocol._
import build.unstable.sonic.model.{OutputChunk, Query, QueryProgress, RequestContext}
import build.unstable.sonicd.model._
import build.unstable.sonicd.source.LocalFileStreamPublisher
import build.unstable.sonicd.source.file.FileWatcher
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, WordSpecLike}
import spray.json._

import scala.concurrent.duration._

class LocalFileSourceSpec(_system: ActorSystem)
  extends TestKit(_system) with WordSpecLike
    with Matchers with BeforeAndAfterAll with ImplicitSender
    with ImplicitSubscriber with HandlerUtils with BeforeAndAfterEach {

  import Fixture._

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    tmp.mkdir()
    file3.createNewFile()
    file4.createNewFile()
    doWrite3("prev\nprev2\n")

    conf.createNewFile()
    conf2.createNewFile()

    doWrite("hellYeahsecretConfig\nsecretConfig\n", confChannel)
    doWrite("hellYeahsecretConfig\nsecretConfig\n", confChannel2)
  }

  override protected def afterAll(): Unit = {
    channel3.close()
    channel4.close()
    confChannel.close()
    confChannel2.close()

    tmp.delete()
    try file3.delete() finally {}
    try file4.delete() finally {}
    TestKit.shutdownActorSystem(system)
    conf.delete()
    conf2.delete()
    super.afterAll()
  }

  def getConfig(path: String, tail: Boolean): JsValue = {
    JsObject(Map(
      "tail" → JsBoolean(tail),
      "path" → JsString(path),
      "class" → JsString("LocalFileStreamSource")
    ))
  }

  val conf = new File("/tmp/application.conf")
  val conf2 = new File("/tmp/reference.conf")

  lazy val confChannel = new FileOutputStream(conf)
  lazy val confChannel2 = new FileOutputStream(conf2)

  def this() = this(ActorSystem("LocalFileSourceSpec"))

  val controller: TestActorRef[TestController] =
    TestActorRef(Props(classOf[TestController], self).withDispatcher(CallingThreadDispatcher.Id))

  val fetchConfig = getConfig(tmp.toPath.toAbsolutePath.toString, tail = false)
  val tailConfig = getConfig(tmp.toPath.toAbsolutePath.toString, tail = true)

  lazy val channel3 = new FileOutputStream(file3)
  lazy val channel4 = new FileOutputStream(file4)

  val dirPath = tmp.toPath
  val filePath3 = file3.toPath
  val filePath4 = file4.toPath

  val MODIFY3 = getEvent(StandardWatchEventKinds.ENTRY_MODIFY, filePath3.getFileName)
  val CREATE3 = getEvent(StandardWatchEventKinds.ENTRY_CREATE, filePath3.getFileName)
  val DELETE3 = getEvent(StandardWatchEventKinds.ENTRY_DELETE, filePath3.getFileName)

  val MODIFY4 = getEvent(StandardWatchEventKinds.ENTRY_MODIFY, filePath4.getFileName)
  val CREATE4 = getEvent(StandardWatchEventKinds.ENTRY_CREATE, filePath4.getFileName)
  val DELETE4 = getEvent(StandardWatchEventKinds.ENTRY_DELETE, filePath4.getFileName)

  def newPublisher(q: String,
                   config: JsValue,
                   watchers: Vector[(File, ActorRef)] = Vector(tmp → self)): ActorRef = {
    val query = new Query(Some(1L), Some("traceId"), None, q, config)
    val src = new LocalFileStreamSource(watchers, query, controller.underlyingActor.context, testCtx)
    val ref = system.actorOf(src.publisher.withDispatcher(CallingThreadDispatcher.Id))
    ActorPublisher(ref).subscribe(subs)
    watch(ref)
    ref
  }

  def doWrite(buf: String, fos: FileOutputStream) = {
    val fd = fos.getFD
    val c = fos.getChannel
    c.force(true)
    fos.write(buf.getBytes(Charset.defaultCharset()))
    fos.flush()
    fd.sync()
  }

  def doWrite4(buf: String) = doWrite(buf, channel4)

  def doWrite3(buf: String) = doWrite(buf, channel3)


  "LocalFileSourceSpec" should {
    "tail files" in {
      val pub = newPublisher("{}", tailConfig)
      pub ! ActorPublisherMessage.Request(1)

      //file watcher to watch for new events
      expectMsgType[FileWatcher.Watch]

      //write data
      doWrite4("test\n")

      pub ! FileWatcher.PathWatchEvent(dirPath, MODIFY4)

      expectStreamStarted()

      pub ! ActorPublisherMessage.Request(1)
      expectTypeMetadata()

      pub ! ActorPublisherMessage.Request(1)
      expectMsg(OutputChunk(Vector("test")))

      doWrite3("test\ntest")

      pub ! FileWatcher.PathWatchEvent(dirPath, MODIFY3)

      pub ! ActorPublisherMessage.Request(1)
      expectMsg(OutputChunk(Vector("test")))


      doWrite3("\ntest\n")

      pub ! FileWatcher.PathWatchEvent(dirPath, MODIFY3)

      pub ! ActorPublisherMessage.Request(2)
      expectMsg(OutputChunk(Vector("test")))
      expectMsg(OutputChunk(Vector("test")))


      doWrite4("test\ntest\ntest\n")

      pub ! FileWatcher.PathWatchEvent(dirPath, MODIFY4)

      pub ! ActorPublisherMessage.Request(3)
      expectMsg(OutputChunk(Vector("test")))
      expectMsg(OutputChunk(Vector("test")))
      expectMsg(OutputChunk(Vector("test")))

      pub ! Cancel
      expectTerminated(pub)
    }

    "fetch files" in {
      val pub = newPublisher("{}", fetchConfig)

      pub ! ActorPublisherMessage.Request(1)
      expectStreamStarted()

      pub ! ActorPublisherMessage.Request(1)
      expectTypeMetadata()

      pub ! ActorPublisherMessage.Request(1)
      expectMsg(OutputChunk(Vector("prev")))
      pub ! ActorPublisherMessage.Request(1)
      expectMsg(OutputChunk(Vector("prev2")))
      pub ! ActorPublisherMessage.Request(1)
      expectMsg(OutputChunk(Vector("test")))
      pub ! ActorPublisherMessage.Request(20)
      expectMsg(OutputChunk(Vector("test")))
      expectMsg(OutputChunk(Vector("test")))

      expectQueryProgress(1, QueryProgress.Running, Some(2), Some("files"))

      expectMsg(OutputChunk(Vector("test")))
      expectMsg(OutputChunk(Vector("test")))
      expectMsg(OutputChunk(Vector("test")))
      expectMsg(OutputChunk(Vector("test")))

      expectQueryProgress(1, QueryProgress.Running, Some(2), Some("files"))

      expectDone(pub)
    }

    "filters files" in {
      // normal value filter
      {
        val pub = newPublisher("""{"filter": {"raw": "prev"}}""", fetchConfig)

        pub ! ActorPublisherMessage.Request(1)
        expectStreamStarted()

        pub ! ActorPublisherMessage.Request(1)
        expectTypeMetadata()

        pub ! ActorPublisherMessage.Request(1)
        expectMsg(OutputChunk(Vector("prev")))
        pub ! ActorPublisherMessage.Request(20)

        expectQueryProgress(1, QueryProgress.Running, Some(2), Some("files"))

        expectQueryProgress(1, QueryProgress.Running, Some(2), Some("files"))

        expectDone(pub)
      }

      // startsWith filter
      {
        val pub = newPublisher("""{"filter": {"raw": "prev*"}}""", fetchConfig)

        pub ! ActorPublisherMessage.Request(1)
        expectStreamStarted()

        pub ! ActorPublisherMessage.Request(1)
        expectTypeMetadata()

        pub ! ActorPublisherMessage.Request(1)
        expectMsg(OutputChunk(Vector("prev")))
        pub ! ActorPublisherMessage.Request(1)
        expectMsg(OutputChunk(Vector("prev2")))
        pub ! ActorPublisherMessage.Request(20)

        expectQueryProgress(1, QueryProgress.Running, Some(2), Some("files"))

        expectQueryProgress(1, QueryProgress.Running, Some(2), Some("files"))

        expectDone(pub)
      }

      // endsWith filter
      {
        val pub = newPublisher("""{"filter": {"raw": "*est"}}""", fetchConfig)

        pub ! ActorPublisherMessage.Request(1)
        expectStreamStarted()

        pub ! ActorPublisherMessage.Request(1)
        expectTypeMetadata()

        pub ! ActorPublisherMessage.Request(1)
        expectMsg(OutputChunk(Vector("test")))
        pub ! ActorPublisherMessage.Request(20)
        expectMsg(OutputChunk(Vector("test")))
        expectMsg(OutputChunk(Vector("test")))

        expectQueryProgress(1, QueryProgress.Running, Some(2), Some("files"))

        expectMsg(OutputChunk(Vector("test")))
        expectMsg(OutputChunk(Vector("test")))
        expectMsg(OutputChunk(Vector("test")))
        expectMsg(OutputChunk(Vector("test")))

        expectQueryProgress(1, QueryProgress.Running, Some(2), Some("files"))

        expectDone(pub)
      }

      // is null filter
      {
        val pub = newPublisher("""{"filter": {"raw": null}}""", fetchConfig)

        pub ! ActorPublisherMessage.Request(1)
        expectStreamStarted()

        pub ! ActorPublisherMessage.Request(20)

        expectQueryProgress(1, QueryProgress.Running, Some(2), Some("files"))

        expectQueryProgress(1, QueryProgress.Running, Some(2), Some("files"))

        expectDone(pub)
      }
    }

    "block streaming of sensitive config files" in {

      val naughtyConfig = getConfig(conf.toPath.toAbsolutePath.toString, tail = true)
      val naughtyConfig2 = getConfig(conf2.toPath.toAbsolutePath.toString, tail = false)

      //tail should just not never stream anything
      {
        val pub = newPublisher("{}", naughtyConfig)
        pub ! ActorPublisherMessage.Request(1)
        //file watcher to watch for new events
        expectMsgType[FileWatcher.Watch]

        //write data
        doWrite("test\n", confChannel)

        pub ! FileWatcher.PathWatchEvent(dirPath, MODIFY3)
        expectStreamStarted()
        pub ! ActorPublisherMessage.Request(1)
        expectNoMsg()

        pub ! Cancel
        expectTerminated(pub, 3.seconds)
      }


      //fetch should complete with no data streamed
      {
        val pub = newPublisher("{}", naughtyConfig2)
        pub ! ActorPublisherMessage.Request(1)
        expectStreamStarted()
        pub ! ActorPublisherMessage.Request(1)
        expectDone(pub)
      }
    }
  }
}

class LocalFileStreamSource(watchers: Vector[(File, ActorRef)], query: Query, actorContext: ActorContext, context: RequestContext)
  extends build.unstable.sonicd.source.LocalFileStreamSource(query, actorContext, context) {

  override lazy val publisher: Props = {
    val path = getConfig[String]("path")
    val tail = getConfig[Boolean]("tail")

    val glob = FileWatcher.parseGlob(path)

    Props(classOf[LocalFileStreamPublisher], query.id.get, query.query,
      tail, glob.fileFilterMaybe, watchers, context)
      .withDispatcher(CallingThreadDispatcher.Id)

  }
}
