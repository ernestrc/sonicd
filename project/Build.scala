import com.typesafe.sbt.SbtGit.GitKeys._
import sbt.Keys._
import sbt._
import sbtassembly.AssemblyKeys._
import sbtassembly.MergeStrategy
import sbtbuildinfo.BuildInfoKeys._
import sbtbuildinfo.BuildInfoPlugin
import sbtbuildinfo.BuildInfoPlugin._
import spray.revolver.RevolverPlugin._

object Build extends sbt.Build {

  val scalaV = "2.11.8"
  val akkaV = "2.4.6"
  val sonicdV = "0.5.2"

  val commonSettings = Seq(
    organization := "build.unstable",
    version := sonicdV,
    scalaVersion := scalaV,
    licenses +=("MIT", url("https://opensource.org/licenses/MIT")),
    resolvers += Resolver.bintrayRepo("ernestrc", "maven"),
    scalacOptions := Seq(
      "-unchecked",
      "-Xlog-free-terms",
      "-deprecation",
      "-feature",
      "-Xlint",
      "-Ywarn-dead-code",
      "-Ywarn-unused",
      "-encoding", "UTF-8",
      "-target:jvm-1.8"
    )
  )

  val meta = """META.INF(.)*""".r

  val assemblyStrategy = assemblyMergeStrategy in assembly := {
    case "reference.conf" => MergeStrategy.concat
    case "application.conf" => MergeStrategy.discard
    case meta(_) => MergeStrategy.discard
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  }

  val core: Project = Project("sonicd-core", file("lib/scala"))
    .settings(commonSettings: _*)
    .settings(
      libraryDependencies ++= {
        Seq(
          "build.unstable" %% "tylog" % "0.2.4",
          "io.spray" %% "spray-json" % "1.3.2",
          "com.typesafe.akka" %% "akka-actor" % akkaV,
          "com.typesafe.akka" %% "akka-slf4j" % akkaV,
          "com.typesafe.akka" %% "akka-stream" % akkaV,
          "com.typesafe.akka" %% "akka-http-spray-json-experimental" % akkaV,
          "org.scalatest" %% "scalatest" % "2.2.5" % "test"
        )
      }
    ).disablePlugins(com.twitter.scrooge.ScroogeSBT)

  val hive: Project = Project("sonicd-hive", file("server/hive"))
    .settings(commonSettings: _*)
    .settings(
      //scroogeBuildOptions in Compile := Seq("--finagle"), //no finagle
      assemblyStrategy,
      libraryDependencies ++= {
        Seq(
          "org.apache.thrift" % "libthrift" % "0.8.0",
          "com.twitter" %% "scrooge-core" % "4.8.0",
          "com.twitter" %% "finagle-thrift" % "6.36.0"
        )
      }).dependsOn(core)

  val server: Project = Project("sonicd-server", file("server"))
    .settings(Revolver.settings: _*)
    .settings(commonSettings: _*)
    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
    .enablePlugins(BuildInfoPlugin)
    .enablePlugins(com.typesafe.sbt.GitVersioning)
    .settings(
      buildInfoKeys ++= Seq[BuildInfoKey](
        version,
        "builtAt" -> {
          val dtf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ")
          dtf.setTimeZone(java.util.TimeZone.getTimeZone("America/Los_Angeles"))
          dtf.format(new java.util.Date())
        },
        "commit" -> gitHeadCommit.value.map(_.take(7)).getOrElse("unknown-commit")),
      buildInfoPackage := "build.unstable.sonicd",
      assemblyStrategy,
      assemblyJarName in assembly := "sonicd-assembly.jar",
      libraryDependencies ++= {
        Seq(
          //core
          "com.typesafe.akka" %% "akka-http-core" % akkaV,
          "com.auth0" % "java-jwt" % "2.1.0",
          "ch.megard" %% "akka-http-cors" % "0.1.2",
          "ch.qos.logback" % "logback-classic" % "1.0.13",
          "com.typesafe.akka" %% "akka-http-testkit" % akkaV % "test",
          "com.h2database" % "h2" % "1.3.175" % "test"
        )
      }
    )
    .disablePlugins(com.twitter.scrooge.ScroogeSBT)
    .dependsOn(core % "compile->compile;test->test", hive)

  val examples = Project("sonicd-examples", file("examples/scala"))
    .settings(Revolver.settings: _*)
    .settings(commonSettings: _*)
    .settings(
      libraryDependencies ++= {
        Seq(
          "build.unstable" %% "tylog-core" % "0.1.3",
          "ch.qos.logback" % "logback-classic" % "1.0.13"
        )
      }
    ).dependsOn(core)
    .disablePlugins(com.twitter.scrooge.ScroogeSBT)
}
