package io.github.stopstreaming.extensions.rest

import io.github.stopstreaming.extensions.conf.RestStopConfig
import io.github.stopstreaming.extensions.StreamingQueryOps._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery
import org.scalatest.funsuite.AnyFunSuite

import java.net.{HttpURLConnection, URL}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.reflect.io.Directory

class RestWatcherTest extends AnyFunSuite {

  val testPort = 18558  // non-default port to avoid clashes in CI

  def sendPost(port: Int, path: String): Int = {
    val url  = new URL(s"http://127.0.0.1:$port$path")
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("POST")
    conn.setDoOutput(true)
    conn.connect()
    val code = conn.getResponseCode
    conn.disconnect()
    code
  }

  def sendGet(port: Int, path: String): Int = {
    val url  = new URL(s"http://127.0.0.1:$port$path")
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("GET")
    conn.connect()
    val code = conn.getResponseCode
    conn.disconnect()
    code
  }

  // -------------------------------------------------------------------------
  // Basic signal
  // -------------------------------------------------------------------------

  test("RestWatcher resolves Future when POST to effectivePath is received") {
    val config  = RestStopConfig(host = "127.0.0.1", port = testPort)
    val watcher = new RestWatcher(config, "job-1")

    assert(watcher.effectivePath == "/stop/job-1")

    watcher.start()
    val signal = watcher.awaitStopSignal()
    Thread.sleep(200)

    assert(sendPost(testPort, "/stop/job-1") == 200)

    val stopped = Await.result(signal, scala.concurrent.duration.Duration(5, "seconds"))
    assert(stopped)
    watcher.shutdown()
  }

  test("RestWatcher returns 405 for non-POST requests") {
    val config  = RestStopConfig(host = "127.0.0.1", port = testPort + 1)
    val watcher = new RestWatcher(config, "job-2")
    watcher.start()
    Thread.sleep(200)

    assert(sendGet(testPort + 1, "/stop/job-2") == 405)
    watcher.shutdown()
  }

  test("RestWatcher resolves Future with false when shutdown before signal") {
    val config  = RestStopConfig(host = "127.0.0.1", port = testPort + 2)
    val watcher = new RestWatcher(config, "job-3")

    watcher.start()
    val signal = watcher.awaitStopSignal()
    Thread.sleep(200)

    watcher.shutdown()  // no POST sent — shutdown cancels the poll loop

    val result = Await.result(signal, scala.concurrent.duration.Duration(5, "seconds"))
    assert(!result)
  }

  test("RestWatcher respects custom stopPath") {
    val config  = RestStopConfig(host = "127.0.0.1", port = testPort + 3, stopPath = "/terminate")
    val watcher = new RestWatcher(config, "job-4")

    assert(watcher.effectivePath == "/terminate/job-4")

    watcher.start()
    val signal = watcher.awaitStopSignal()
    Thread.sleep(200)

    assert(sendPost(testPort + 3, "/terminate/job-4") == 200)

    val result = Await.result(signal, scala.concurrent.duration.Duration(5, "seconds"))
    assert(result)
    watcher.shutdown()
  }

  // -------------------------------------------------------------------------
  // Multi-job: two watchers, each stopped independently by job ID
  // -------------------------------------------------------------------------

  test("two watchers on different ports stop independently by jobId") {
    val cfgA = RestStopConfig(host = "127.0.0.1", port = testPort + 4)
    val cfgB = RestStopConfig(host = "127.0.0.1", port = testPort + 5)

    val watcherA = new RestWatcher(cfgA, "job-A")
    val watcherB = new RestWatcher(cfgB, "job-B")

    watcherA.start()
    watcherB.start()

    val signalA = watcherA.awaitStopSignal()
    val signalB = watcherB.awaitStopSignal()

    Thread.sleep(200)

    // Stop only job A — B must remain active
    assert(sendPost(testPort + 4, "/stop/job-A") == 200)
    val resultA = Await.result(signalA, scala.concurrent.duration.Duration(5, "seconds"))
    assert(resultA)
    assert(!signalB.isCompleted)

    // Now stop job B
    assert(sendPost(testPort + 5, "/stop/job-B") == 200)
    val resultB = Await.result(signalB, scala.concurrent.duration.Duration(5, "seconds"))
    assert(resultB)

    watcherA.shutdown()
    watcherB.shutdown()
  }

  // -------------------------------------------------------------------------
  // End-to-end with a real Spark streaming query
  // -------------------------------------------------------------------------

  test("stop streaming query via REST backend end-to-end") {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("RestWatcherTest")
      .getOrCreate()

    val tmpInput  = "/tmp/rest_watcher_test/input"
    val tmpOutput = "/tmp/rest_watcher_test/output"
    writeTestCsv(tmpInput)

    val schema = org.apache.spark.sql.types.StructType(Seq(
      org.apache.spark.sql.types.StructField("value", org.apache.spark.sql.types.StringType)
    ))

    val query: StreamingQuery = spark.readStream
      .schema(schema)
      .csv(tmpInput)
      .writeStream
      .format("csv")
      .option("path", tmpOutput)
      .option("checkpointLocation", s"$tmpOutput/_checkpoint")
      .start()

    query.awaitTermination(2000)

    val config = RestStopConfig(host = "127.0.0.1", port = testPort + 6)

    val terminationFuture = Future {
      query.awaitExternalTermination(config)
    }

    Thread.sleep(500)
    assert(sendPost(testPort + 6, s"/stop/${query.id}") == 200)

    Await.result(terminationFuture, scala.concurrent.duration.Duration(10, "seconds"))
    assert(!query.isActive)

    spark.stop()
    Directory(new java.io.File("/tmp/rest_watcher_test")).deleteRecursively()
  }

  private def writeTestCsv(dir: String): Unit = {
    new java.io.File(dir).mkdirs()
    val pw = new java.io.PrintWriter(s"$dir/data.csv")
    pw.println("hello")
    pw.println("world")
    pw.close()
  }
}
