name := "csv-cut"

version := "1.0"

scalaVersion := "2.11.7"

scalacOptions ++= Seq("-feature")


libraryDependencies ++= Seq(
  "com.github.tototoshi" %% "scala-csv" % "1.2.2",
  "com.github.scopt" %% "scopt" % "3.2.0"
)
