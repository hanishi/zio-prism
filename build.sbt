ThisBuild / scalaVersion := "3.3.4"
ThisBuild / organization := "llc.programmer"
ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / licenses     := Seq("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0"))

val zioVersion = "2.1.26"

// The library: a clean-room, Chunk[Byte]-native reimplementation of the prism
// streaming-rewrite engine on ZIO Streams (Aho-Corasick / Boyer-Moore-Horspool / Wu-Manber,
// the (input, atEOF) => (output, consumed) carry contract). Depends only on zio-streams.
lazy val root = (project in file("."))
  .settings(
    name := "zio-prism",
    scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked"),
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio-streams"  % zioVersion,
      "dev.zio" %% "zio-test"     % zioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % zioVersion % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )

// Runnable samples: pull a large doc / an endless feed over HTTP and rewrite it as it
// streams. Depends on the library; never published and not aggregated by root, so it stays
// out of the published jar.  Run:  sbt "examples/runMain prism.SampleApp"
lazy val examples = (project in file("examples"))
  .dependsOn(root)
  .settings(
    name                    := "zio-prism-examples",
    publish / skip          := true,
    Compile / doc / sources := Seq.empty,
    // Run examples in a forked JVM so Ctrl-C (SIGINT) is delivered to the app's own process and
    // ZIOAppDefault's interrupt handler fires. Without forking, runMain shares sbt's JVM and sbt
    // intercepts Ctrl-C, so the app never sees the shutdown signal and appears to hang.
    Compile / run / fork         := true,
    Compile / run / connectInput := true,
    outputStrategy               := Some(StdoutOutput)
  )

// JMH throughput benchmarks for the engine and the streaming pipeline. Depends on the
// library; never published and not aggregated by root, so compile/test/publish don't touch
// it.  Run:  sbt "bench/Jmh/run"
lazy val bench = (project in file("bench"))
  .dependsOn(root)
  .enablePlugins(JmhPlugin)
  .settings(
    name                    := "zio-prism-bench",
    publish / skip          := true,
    Compile / doc / sources := Seq.empty
  )
