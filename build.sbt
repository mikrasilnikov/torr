ThisBuild / scalaVersion := "2.13.6"
ThisBuild / version := "1.0.0"
// ThisBuild / organization := "com.example"
// ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "tor",
    libraryDependencies ++= Seq(
      "dev.zio"       %% "zio"          % "1.0.9",
      "dev.zio"       %% "zio-nio-core" % "1.0.0-RC11",
      "dev.zio"       %% "zio-nio"      % "1.0.0-RC11",
      "org.typelevel" %% "cats-core"    % "2.6.1",
      "dev.zio"       %% "zio-test"     % "1.0.9" % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )
