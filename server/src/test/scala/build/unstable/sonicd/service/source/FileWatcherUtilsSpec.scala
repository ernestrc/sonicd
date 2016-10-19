package build.unstable.sonicd.service.source

import java.io.File

import build.unstable.sonicd.model.Fixture
import build.unstable.sonicd.source.file.FileWatcher
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, WordSpec}

class FileWatcherUtilsSpec extends WordSpec with Matchers
with BeforeAndAfterAll with BeforeAndAfterEach {
  import Fixture._

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    tmp.mkdir()
    tmp2.mkdir()
    tmp3.mkdir()
    tmp32.mkdir()
    tmp4.mkdir()
    file.createNewFile()
    file2.createNewFile()
    file3.createNewFile()
  }

  override protected def afterAll(): Unit = {
    tmp.delete()
    tmp2.delete()
    tmp3.delete()
    tmp32.delete()
    tmp4.delete()
    file.delete()
    file2.delete()
    file3.delete()
    super.afterAll()
  }

  "FileWatcher utils" should {
    "parse glob" in {
      val glob1 = FileWatcher.parseGlob(s"$tmp")

      glob1.folders should contain theSameElementsAs Seq(
        new File(s"$tmp").toPath
      )

      /* FIXME not supported
      val glob11 = FileWatcher.parseGlob(s"$tmp/rec*")

      glob11.folders should contain theSameElementsAs Seq(
        new File(s"$tmp").toPath,
        new File(s"$tmp/recursive").toPath,
        new File(s"$tmp/recursive2").toPath
      )

      assert(glob11.fileFilterMaybe.nonEmpty)
      glob11.fileFilterMaybe.get shouldBe "*.xml"
      */


      val glob2 = FileWatcher.parseGlob(s"$tmp/*.xml")

      glob2.folders should contain theSameElementsAs Seq(
        new File(s"$tmp").toPath
      )


      val glob21 = FileWatcher.parseGlob(s"$tmp/logback.xml")

      glob21.folders should contain theSameElementsAs Seq(
        new File(s"$tmp").toPath
      )

      assert(glob21.fileFilterMaybe.nonEmpty)
      glob21.fileFilterMaybe.get shouldBe "logback.xml"


      val glob22 = FileWatcher.parseGlob(s"$tmp/**/logback.xml")

      glob22.folders should contain theSameElementsAs Seq(
        new File(s"$tmp/recursive").toPath,
        new File(s"$tmp/recursive/rec2").toPath,
        new File(s"$tmp/recursive/rec2/rec2").toPath,
        new File(s"$tmp/recursive2").toPath,
        new File(s"$tmp").toPath
      )

      assert(glob22.fileFilterMaybe.nonEmpty)
      glob22.fileFilterMaybe.get shouldBe "logback.xml"


      val glob3 = FileWatcher.parseGlob(s"$tmp/**")

      glob3.folders should contain theSameElementsAs Seq(
        new File(s"$tmp/recursive").toPath,
        new File(s"$tmp/recursive/rec2").toPath,
        new File(s"$tmp/recursive/rec2/rec2").toPath,
        new File(s"$tmp/recursive2").toPath,
        new File(s"$tmp").toPath
      )


      val glob4 = FileWatcher.parseGlob(s"$tmp/**/*.xml")

      glob4.folders should contain theSameElementsAs Seq(
        new File(s"$tmp/recursive").toPath,
        new File(s"$tmp/recursive/rec2").toPath,
        new File(s"$tmp/recursive/rec2/rec2").toPath,
        new File(s"$tmp/recursive2").toPath,
        new File(s"$tmp").toPath
      )

      assert(glob4.fileFilterMaybe.nonEmpty)
      glob4.fileFilterMaybe.get shouldBe "*.xml"


      val glob5 = FileWatcher.parseGlob(s"$tmp/**/")

      glob5.folders should contain theSameElementsAs Seq(
        new File(s"$tmp/recursive").toPath,
        new File(s"$tmp/recursive/rec2").toPath,
        new File(s"$tmp/recursive/rec2/rec2").toPath,
        new File(s"$tmp/recursive2").toPath,
        new File(s"$tmp").toPath
      )


      /* FIXME
      val glob6 = FileWatcher.parseGlob(s"$tmp/**/rec2/tmp.txt")

      glob6.folders should contain theSameElementsAs Seq(
        new File(s"$tmp/recursive/rec2").toPath,
        new File(s"$tmp/recursive/rec2/rec2").toPath
      )

      assert(glob6.fileFilterMaybe.nonEmpty)
      glob6.fileFilterMaybe.get shouldBe "tmp.txt"
      */


      //val glob7 = FileWatcher.parseGlob(s"$tmp/**/rec2/*.txt")
      /* FIXME

      glob7.folders should contain theSameElementsAs Seq(
        new File(s"$tmp/recursive/rec2").toPath,
        new File(s"$tmp/recursive/rec2/rec2").toPath
      )

      assert(glob7.fileFilterMaybe.nonEmpty)
      glob7.fileFilterMaybe.get shouldBe "*.txt"
      */


      //val glob8 = FileWatcher.parseGlob(s"$tmp/**/rec2/rec2/*.txt")

      /* FIXME
      glob8.folders should contain theSameElementsAs Seq(
        new File(s"$tmp/recursive/rec2/rec2").toPath,
        new File(s"$tmp").toPath
      )

      assert(glob8.fileFilterMaybe.nonEmpty)
      glob8.fileFilterMaybe.get shouldBe "*.txt"
      */

    }

    "throw exception when glob doesn't match any folders" in {

      intercept[AssertionError] {
        FileWatcher.parseGlob(s"$tmp/recursive/oopsie")
      }

      intercept[AssertionError] {
        FileWatcher.parseGlob("")
      }
    }
  }
}
