addSbtPlugin("pl.project13.scala" % "sbt-jmh"        % "0.4.7")
// Publishing: derives version from git tags (sbt-dynver), signs (sbt-pgp), and publishes to the
// Sonatype Central Portal (sbt-sonatype). `sbt ci-release` ties them together for tag-driven CI.
addSbtPlugin("com.github.sbt"     % "sbt-ci-release" % "1.11.2")
