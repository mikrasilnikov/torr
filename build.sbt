ThisBuild / scalaVersion := "2.13.6"
ThisBuild / version := "1.0.0"
// ThisBuild / organization := "com.example"
// ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "torr",
    libraryDependencies ++= Seq(
      "dev.zio"              %% "zio"               % "1.0.11",
      "dev.zio"              %% "zio-nio-core"      % "1.0.0-RC11",
      "dev.zio"              %% "zio-nio"           % "1.0.0-RC11",
      "dev.zio"              %% "zio-macros"        % "1.0.11",
      "dev.zio"              %% "zio-logging-slf4j" % "0.5.10",
      "ch.qos.logback"        % "logback-classic"   % "1.2.3",
      "dev.zio"              %% "zio-actors"        % "0.0.9",
      "dev.zio"              %% "ziocli"            % "0.0.0+98-5435a0aa-SNAPSHOT",
      "io.github.kitlangton" %% "zio-magic"         % "0.3.8",
      "dev.zio"              %% "zio-test-sbt"      % "1.0.11" % "test",
      "dev.zio"              %% "zio-test"          % "1.0.11" % Test
    )
  )

scalacOptions += "-Ymacro-annotations"

testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
