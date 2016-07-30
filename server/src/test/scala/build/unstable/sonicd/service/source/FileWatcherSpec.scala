package build.unstable.sonicd.service.source

import java.io.File
import java.nio.file.StandardWatchEventKinds

import akka.actor.{Terminated, PoisonPill, ActorSystem, Props}
import akka.testkit.{CallingThreadDispatcher, ImplicitSender, TestActorRef, TestKit}
import build.unstable.sonicd.service.{Fixture, ImplicitRedirectActor, ImplicitSubscriber}
import build.unstable.sonicd.source.file.FileWatcher.{PathWatchEvent, Watch}
import build.unstable.sonicd.source.file.{FileWatcherWorker, FileWatcher}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

class FileWatcherSpec(_system: ActorSystem)
  extends TestKit(_system) with WordSpecLike
  with Matchers with BeforeAndAfterAll with ImplicitSender
  with ImplicitSubscriber with HandlerUtils {

  import Fixture._

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    testDir.delete()
  }

  override protected def beforeAll(): Unit = {
    testDir.createNewFile()
  }

  def this() = this(ActorSystem("FileWatcherSpec"))

  val controller: TestActorRef[TestController] =
    TestActorRef(Props(classOf[TestController], self).
      withDispatcher(CallingThreadDispatcher.Id))

  val testDir = new File("/tmp/sonicd_watcher_spec")
  val testFile = new File("/tmp/sonicd_watcher_spec/test.json")

  val MODIFY = getEvent(StandardWatchEventKinds.ENTRY_MODIFY, testFile.toPath.getFileName)
  val pathEventModify = PathWatchEvent(testDir.toPath, MODIFY)
  val CREATE = getEvent(StandardWatchEventKinds.ENTRY_CREATE, testFile.toPath.getFileName)
  val pathEventCreate = PathWatchEvent(testDir.toPath, CREATE)
  val DELETE = getEvent(StandardWatchEventKinds.ENTRY_DELETE, testFile.toPath.getFileName)
  val pathEventDelete = PathWatchEvent(testDir.toPath, DELETE)

  def newWatcher = {
    system.actorOf(Props(classOf[FileWatcher],
      testDir.toPath, Props(classOf[ImplicitRedirectActor], self).withDispatcher(CallingThreadDispatcher.Id)
    ).withDispatcher(CallingThreadDispatcher.Id))
  }

  def newProxy = system.actorOf(Props(classOf[ImplicitRedirectActor], self).withDispatcher(CallingThreadDispatcher.Id))


  "FileWatcher" should {
    "subscribe subscribers to new events and send events from workers" in {
      val watcher = newWatcher
      watch(watcher)
      val proxy1 = newProxy
      val proxy2 = newProxy
      expectMsg(FileWatcherWorker.DoWatch)

      //3 subscribers
      watcher ! Watch(None, Fixture.testCtx)
      watcher.tell(Watch(None, Fixture.testCtx), proxy1)
      watcher.tell(Watch(None, Fixture.testCtx), proxy2)

      watcher ! FileWatcher.WatchResults(MODIFY :: Nil)
      val msgs = receiveN(4)
      msgs.count(_ == FileWatcherWorker.DoWatch) shouldBe 1
      msgs.count(_ == pathEventModify) shouldBe 3

      proxy1 ! PoisonPill
      Thread.sleep(1000) //calling thread dispatcher is awesome but not enough

      watcher ! FileWatcher.WatchResults(MODIFY :: Nil)
      val msgs2 = receiveN(3)
      msgs2.count(_ == FileWatcherWorker.DoWatch) shouldBe 1
      msgs2.count(_ == pathEventModify) shouldBe 2

      proxy2 ! PoisonPill
      Thread.sleep(1000)

      watcher ! FileWatcher.WatchResults(MODIFY :: Nil)
      expectMsgAllOf(pathEventModify, FileWatcherWorker.DoWatch)
      expectNoMsg()
      watcher ! PoisonPill
      expectTerminated(watcher)
    }

    "filter events that subscribers" in {
      {
        val watcher = newWatcher
        expectMsg(FileWatcherWorker.DoWatch)

        watcher ! Watch(Some("test2.json"), Fixture.testCtx)

        watcher ! FileWatcher.WatchResults(MODIFY :: Nil)
        expectMsg(FileWatcherWorker.DoWatch)
        expectNoMsg()
        watcher ! PoisonPill
      }

      {
        val watcher = newWatcher
        expectMsg(FileWatcherWorker.DoWatch)

        watcher ! Watch(Some("test.json"), Fixture.testCtx)

        watcher ! FileWatcher.WatchResults(MODIFY :: Nil)
        expectMsgAllOf(pathEventModify, FileWatcherWorker.DoWatch)
        expectNoMsg()
        watcher ! PoisonPill
      }
    }
  }
}
