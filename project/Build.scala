import com.typesafe.sbt.SbtGit.GitKeys._
import sbt.Keys._
import sbt._
import sbtassembly.AssemblyKeys._
import sbtassembly.{PathList, MergeStrategy}
import sbtbuildinfo.BuildInfoKeys._
import sbtbuildinfo.BuildInfoPlugin
import sbtbuildinfo.BuildInfoPlugin._
import spray.revolver.RevolverPlugin._

object Build extends sbt.Build {

  val scalaV = "2.11.8"
  val akkaV = "2.4.11"
  val sonicdV = "0.6.7"

  val commonSettings = Seq(
    organization := "build.unstable",
    version := sonicdV,
    scalaVersion := scalaV,
    licenses += ("MIT", url("https://opensource.org/licenses/MIT")),
    resolvers += Resolver.bintrayRepo("ernestrc", "maven"),
    publishArtifact in(Compile, packageDoc) := false,
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
          "build.unstable" %% "sonic-core" % sonicdV,
          "com.typesafe.akka" %% "akka-http-core" % akkaV,
          "com.auth0" % "java-jwt" % "2.1.0",
          "net.logstash.logback" % "logstash-logback-encoder" % "4.7",
          "ch.qos.logback" % "logback-classic" % "1.1.7",
          "com.h2database" % "h2" % "1.3.175" % "test",
          "com.typesafe.akka" %% "akka-http-testkit" % akkaV % "test",
          "org.scalatest" %% "scalatest" % "2.2.5" % "test"
        )
      }
    )
}
