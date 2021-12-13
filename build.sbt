ThisBuild / scalaVersion := "2.13.6"
ThisBuild / version := "0.0.1"

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
      "dev.zio"              %% "ziocli"            % "0.0.0+109-6dd4538e-SNAPSHOT",
      "io.github.kitlangton" %% "zio-magic"         % "0.3.8",
      "dev.zio"              %% "zio-test-sbt"      % "1.0.11" % "test",
      "org.fusesource.jansi"  % "jansi"             % "2.3.2",
      "dev.zio"              %% "zio-test"          % "1.0.11" % Test
    )
  )

assembly / mainClass := Some("torr.Main")
assembly / assemblyJarName := "torr.jar"

scalacOptions += "-Ymacro-annotations"

testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
