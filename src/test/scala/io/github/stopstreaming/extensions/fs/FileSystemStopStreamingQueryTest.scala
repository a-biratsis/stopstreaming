package io.github.stopstreaming.extensions.fs

import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}

import io.github.stopstreaming.extensions.StreamingQueryOps._
import io.github.stopstreaming.extensions.conf.FileSystemStopConfig
import org.apache.spark.sql.{Encoders, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import scala.reflect.io.Directory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

case class RowData(c1: Int, c2: Int, c3: String)

class FileSystemStopStreamingQueryTest extends AnyFlatSpec {
  lazy val spark = SparkSession
    .builder()
    .appName("test")
    .master("local[*]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._

  private val tmpPath: String       = "/tmp/stop_streaming/"
  private val sampleDataDir: String = s"${tmpPath}data"

  // Separate from tmpPath so cleanUpDir() on the marker dir
  // does not delete the data/output directories Spark is still using.
  private val stopMarkerDir: String = "/tmp/stop_streaming_markers/"

  def cleanUpDir(path: String): Unit = {
    val directory = new Directory(new File(path))
    directory.deleteRecursively()
    directory.createDirectory(true, false)
  }

  def writeResourceToTmp(): Unit = {
    val resourceStream = this.getClass.getResourceAsStream("/stream_data.csv")
    if (resourceStream == null)
      throw new RuntimeException("Resource /stream_data.csv not found on classpath")
    val dir = new File(sampleDataDir)
    if (!dir.exists()) dir.mkdirs()
    val target = Paths.get(sampleDataDir, "stream_data.csv")
    Files.copy(resourceStream, target, StandardCopyOption.REPLACE_EXISTING)
  }

  it should "stop existing streaming job" in {
    cleanUpDir(tmpPath)
    writeResourceToTmp()

    val streamingQuery = spark
      .readStream
      .schema(Encoders.product[RowData].schema)
      .option("header", value = false)
      .option("delimiter", value = ";")
      .csv(sampleDataDir)
      .as[RowData]

    val q = streamingQuery.writeStream
      .outputMode("append")
      .format("csv")
      .option("checkpointLocation", tmpPath)
      .option("path", tmpPath)
      .start()

    val stopStreamingPath = s"$stopMarkerDir/${q.id.toString}"

    while (q.isActive && q.recentProgress.length <= 0) {
      Thread.sleep(100)
    }

    cleanUpDir(stopMarkerDir)

    val stopFile = new File(stopStreamingPath)

    val removeStopFileFuture: Future[Boolean] = Future {
      while (!stopFile.exists()) {
        Thread.sleep(100)
      }
      stopFile.delete()
      true
    }

    removeStopFileFuture.onComplete {
      case Success(_) => println(s"$stopStreamingPath was successfully removed.")
      case Failure(_) => fail(s"failed to remove: $stopStreamingPath.")
    }

    val config = FileSystemStopConfig(stopDir = stopMarkerDir)
    q.awaitExternalTermination(config)

    assert(q.isActive == false)
  }
}
