package build.unstable.sonicd.service.source

import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.charset.Charset
import java.nio.file.WatchEvent.Kind
import java.nio.file._

import akka.actor._
import akka.stream.actor.ActorPublisherMessage.Cancel
import akka.stream.actor.{ActorPublisher, ActorPublisherMessage}
import akka.testkit.{CallingThreadDispatcher, ImplicitSender, TestActorRef, TestKit}
import build.unstable.sonicd.model.JsonProtocol._
import build.unstable.sonicd.model.{OutputChunk, Query, RequestContext}
import build.unstable.sonicd.service.{Fixture, ImplicitSubscriber}
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
    doWrite("prev\nprev2\n")

    conf.createNewFile()
    conf2.createNewFile()

    doWrite("hellYeah", confChannel)
    doWrite("hellYeah", confChannel2)
  }

  override protected def afterAll(): Unit = {
    f.close()
    confChannel.close()
    confChannel2.close()

    tmp.delete()
    try file3.delete() finally {}
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

  lazy val confChannel = Files.newByteChannel(conf.toPath, StandardOpenOption.WRITE)
  lazy val confChannel2 = Files.newByteChannel(conf2.toPath, StandardOpenOption.WRITE)

  def this() = this(ActorSystem("LocalFileSourceSpec"))

  val controller: TestActorRef[TestController] =
    TestActorRef(Props(classOf[TestController], self).withDispatcher(CallingThreadDispatcher.Id))

  val fetchConfig = getConfig(tmp.toPath.toAbsolutePath.toString, tail = false)
  val tailConfig = getConfig(tmp.toPath.toAbsolutePath.toString, tail = true)

  lazy val f =
    Files.newByteChannel(file3.toPath.toAbsolutePath, StandardOpenOption.SYNC, StandardOpenOption.WRITE, StandardOpenOption.APPEND, StandardOpenOption.DSYNC)
  val dirPath = tmp.toPath
  val filePath = file3.toPath

  val MODIFY = getEvent(StandardWatchEventKinds.ENTRY_MODIFY, filePath.getFileName)
  val CREATE = getEvent(StandardWatchEventKinds.ENTRY_CREATE, filePath.getFileName)
  val DELETE = getEvent(StandardWatchEventKinds.ENTRY_DELETE, filePath.getFileName)

  def newPublisher(q: String,
                   config: JsValue,
                   watchers: Vector[(File, ActorRef)] = Vector(tmp → self)): ActorRef = {
    val query = new Query(Some(1L), Some("traceId"), None, q, config)
    val src = new LocalFileStreamSource(watchers, query, controller.underlyingActor.context, testCtx)
    val ref = system.actorOf(src.handlerProps.withDispatcher(CallingThreadDispatcher.Id))
    ActorPublisher(ref).subscribe(subs)
    watch(ref)
    ref
  }

  def doWrite(buf: String, channel: SeekableByteChannel = f) = {
    channel.write(ByteBuffer.wrap(buf.getBytes(Charset.defaultCharset())))
  }


  "LocalFileSourceSpec" should {
    "tail files" in {
      val pub = newPublisher("{}", tailConfig)
      pub ! ActorPublisherMessage.Request(1)

      //file watcher to watch for new events
      expectMsgType[FileWatcher.Watch]

      //write data
      doWrite("test\n")

      pub ! FileWatcher.PathWatchEvent(dirPath, MODIFY)

      expectTypeMetadata()
      pub ! ActorPublisherMessage.Request(1)
      expectMsg(OutputChunk(Vector("test")))

      pub ! Cancel
      expectTerminated(pub)
    }

    "fetch files" in {
      val pub = newPublisher("{}", fetchConfig)
      pub ! ActorPublisherMessage.Request(1)

      expectTypeMetadata()
      pub ! ActorPublisherMessage.Request(1)
      expectMsg(OutputChunk(Vector("prev")))
      pub ! ActorPublisherMessage.Request(1)
      expectMsg(OutputChunk(Vector("prev2")))
      pub ! ActorPublisherMessage.Request(1)
      expectMsg(OutputChunk(Vector("test")))
      pub ! ActorPublisherMessage.Request(1)
      expectDone(pub)
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
        doWrite("test\n")

        pub ! FileWatcher.PathWatchEvent(dirPath, MODIFY)
        expectNoMsg()

        pub ! Cancel
        expectTerminated(pub, 3.seconds)
      }


      //fetch should complete with no data streamed
      {
        val pub = newPublisher("{}", naughtyConfig2)
        pub ! ActorPublisherMessage.Request(1)
        expectDone(pub)
      }
    }
  }
}

class LocalFileStreamSource(watchers: Vector[(File, ActorRef)], query: Query, actorContext: ActorContext, context: RequestContext)
  extends build.unstable.sonicd.source.LocalFileStreamSource(query, actorContext, context) {

  override lazy val handlerProps: Props = {
    val path = getConfig[String]("path")
    val tail = getConfig[Boolean]("tail")

    val glob = FileWatcher.parseGlob(path)

    Props(classOf[LocalFileStreamPublisher], query.id.get, query.query,
      tail, glob.fileFilterMaybe, watchers, context)
      .withDispatcher(CallingThreadDispatcher.Id)

  }
}
