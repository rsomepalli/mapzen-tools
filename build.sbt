import Dependencies._
name := """mapzen_location_indexer"""

version := "1.0"

scalaVersion := "2.11.7"



lazy val commonSettings = Seq(
  organization := "org.lakumbra",
  version := "0.1",
  scalaVersion := "2.11.8",
   scalastyleFailOnError := false,
  parallelExecution in Test := false,
  logBuffered in Test := false,
  scalacOptions ++= Seq(
    "-feature",
    "-deprecation",
    "-encoding", "UTF-8", // yes, this is 2 args
    "-unchecked",
    "-Xfatal-warnings",
    "-Yno-adapted-args",
    "-Ywarn-dead-code", // N.B. doesn't work well with the ??? hole
    // "-Ywarn-numeric-widen",
    "-Ywarn-value-discard",
    // "-Ywarn-unused-import",
    "-Xfuture",
    "-language:existentials",
    "-language:higherKinds",
    "-language:postfixOps",
    "-language:implicitConversions"
  ),
  // show elapsed time
  testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oDF"),
  // don't run tests in assembly
  test in assembly := {}
)


// Parent project allows compiling and testing all submodules from one place
lazy val pipeline = project.in(file("."))
  .aggregate( extractors)
  .settings(commonSettings: _*)

// All extraction jobs
lazy val extractors = project.in(file("extractors"))
//  .dependsOn(core)
  .settings(commonSettings: _*)
  .settings(
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
  ).settings(
  libraryDependencies ++= Seq(
    sparkCore,
    sparkSql,
    sparkml,
    akkaActor,
    akkaSlf4j,
    sparkRedshift,
    sparkCsv,
    sparkTs,
    htmlCleaner,
    mysql,
    postgres,
    slf4jlog4j,
    pdfbox,
    pdfbox_tools,
    bcmail,
    bcprov,
    bcpkix,
    "de.julielab" % "aliasi-lingpipe" % "4.1.0",
    scalatest % Test,
    akkaTestkit % Test,
    h2 % Test
    , scalaopt
    ,es
  )
)
