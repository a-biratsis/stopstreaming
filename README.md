# Stop Streaming Gracefully

The purpose of this project is to stop a Spark Structured Streaming job through the file system. At the moment, Databricks DBFS and local file system are supported.

The need arises from the fact that accessing the Spark context from a notebook in order to call the `stop` method on a `StreamingQuery` is often impractical or impossible in managed environments such as Databricks.

The solution is based on a file watcher (Scala `Future`) that runs asynchronously and keeps the streaming job running as long as a corresponding marker file exists in a predefined directory. When the file is deleted, the `stop` method is called, stopping the query gracefully. This allows you to control the lifetime of a streaming job via a shared directory without direct access to the Spark context.

The implementation extends the built-in Spark class `StreamingQuery` with the method `awaitExternalTermination(streamStopDir, jobName, fsType)`.

## Requirements

- Java 17+
- Scala 2.13
- Apache Spark 3.5.x
- SBT 1.9+

## Build

### Compile

```bash
sbt compile
```

### Run tests

```bash
sbt test
```

### Package a JAR

```bash
sbt package
```

The JAR is produced at `target/scala-2.13/stopstreaminggracefully_2.13-0.1.jar`.

### Create a fat JAR (assembly)

If you need a self-contained JAR with all dependencies included, add the [sbt-assembly](https://github.com/sbt/sbt-assembly) plugin to `project/plugins.sbt`:

```scala
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.2.0")
```

Then run:

```bash
sbt assembly
```

## Usage

Write your streaming program and call `awaitExternalTermination` instead of `awaitTermination`, passing the following arguments:

- `streamStopDir` — the directory to watch for the marker file
- `jobName` — a unique identifier for the job, used as the marker file name inside `streamStopDir`
- `fsType` — one of `FileSystemType.DBFS` or `FileSystemType.LocalFileSystem`

To stop the job, delete the marker file. From a Databricks notebook or CLI:

```scala
%fs rm -r your_path
```

Or from a local shell:

```bash
rm /your/stop/dir/<job-id>
```

## Scala example

```scala
import com.abiratsis.spark.streaming.extensions.extensions._

val streamingQuery = spark
  .readStream
  .csv("some_path")

val sq = streamingQuery.writeStream
  .outputMode("append")
  .format("csv")
  .option("path", "some_path")
  .start()

val stopStreamingDir = "some_dbfs_path"

sq.awaitExternalTermination(stopStreamingDir, sq.id.toString, FileSystemType.DBFS)
```

For a local file system:

```scala
sq.awaitExternalTermination("/tmp/stop_streaming/", sq.id.toString, FileSystemType.LocalFileSystem)
```
