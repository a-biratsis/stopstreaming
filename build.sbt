name := "StopStreamingGracefully"

version := "0.1"

scalaVersion := "2.13.17"

val sparkVersion = "4.1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql"  % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.scalatest"    %% "scalatest"  % "3.2.19" % Test,
  "org.scalactic"    %% "scalactic"  % "3.2.19",
  "com.typesafe"     %  "config"     % "1.4.3",
  "com.databricks"   %  "dbutils-api_2.12" % "0.0.5" % Provided excludeAll(
    ExclusionRule(organization = "org.apache.spark")
  )
)

// Expose JDK's built-in HttpServer (jdk.httpserver module) to user code.
// Required by RestWatcher at both compile-time and run-time.
scalacOptions += "-J--add-exports=jdk.httpserver/com.sun.net.httpserver=ALL-UNNAMED"

Test / logBuffered := false
Test / fork        := true
Test / javaOptions ++= Seq(
  "--add-exports=jdk.httpserver/com.sun.net.httpserver=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
)
