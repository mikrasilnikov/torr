ThisBuild / scalaVersion := "2.13.6"
ThisBuild / version := "1.0.0"
// ThisBuild / organization := "com.example"
// ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "torr",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio"          % "1.0.9",
      "dev.zio" %% "zio-nio-core" % "1.0.0-RC11",
      "dev.zio" %% "zio-nio"      % "1.0.0-RC11",
      //"io.d11"  %% "zhttp"        % "1.0.0.0-RC17",
      "dev.zio" %% "zio-test-sbt" % "1.0.9" % "test",
      "dev.zio" %% "zio-test"     % "1.0.9" % Test
    )
  )

testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
