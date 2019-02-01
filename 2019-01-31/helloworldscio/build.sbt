name := "helloworldscio"

version := "0.1"

scalaVersion := "2.12.8"

lazy val helloworld = (project in file("."))
  .settings(
    name := "helloworld",
    libraryDependencies ++= Seq(
      "org.slf4j" % "slf4j-simple" % "1.7.25",
      "com.spotify" %% "scio-core" % "0.7.0",
      "org.apache.beam" % "beam-runners-direct-java" % "2.9.0",
      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % "2.9.0"
    )
  )
