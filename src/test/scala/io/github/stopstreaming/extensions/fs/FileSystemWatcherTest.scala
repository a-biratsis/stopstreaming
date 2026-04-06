package io.github.stopstreaming.extensions.fs

import io.github.stopstreaming.extensions.conf.{FileSystemStopConfig, FsType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach
import java.io.File
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class FileSystemWatcherTest extends AnyFunSuite with BeforeAndAfterEach {

  private val testDir    = "/tmp/fs_watcher_test"
  private val jobName    = "test-job"
  private val markerPath = s"$testDir/$jobName"

  override def beforeEach(): Unit = new File(testDir).mkdirs()

  override def afterEach(): Unit = {
    val f = new File(markerPath)
    if (f.exists()) f.delete()
  }

  // spark = null in all unit tests — no SparkSession needed.
  // The listener is skipped; shutdown() drives the running flag instead.

  // -------------------------------------------------------------------------
  // start()
  // -------------------------------------------------------------------------

  test("start() creates the marker file when absent") {
    val watcher = new FileSystemWatcher(FileSystemStopConfig(testDir), jobName)
    watcher.start()
    assert(new File(markerPath).exists())
    watcher.shutdown()
  }

  test("start() does not overwrite marker file when already present") {
    new java.io.PrintWriter(markerPath) { write("pre-existing"); close() }

    val watcher = new FileSystemWatcher(FileSystemStopConfig(testDir), jobName)
    watcher.start()

    val content = scala.io.Source.fromFile(markerPath).mkString
    assert(content == "pre-existing")
    watcher.shutdown()
  }

  // -------------------------------------------------------------------------
  // awaitStopSignal() — stop signal received (file deleted)
  // -------------------------------------------------------------------------

  test("awaitStopSignal returns true when marker file is deleted") {
    val watcher = new FileSystemWatcher(FileSystemStopConfig(testDir), jobName)
    watcher.start()
    val signal = watcher.awaitStopSignal()

    new Thread(() => {
      Thread.sleep(300)
      new File(markerPath).delete()
    }).start()

    val result = Await.result(signal, 5.seconds)
    assert(result)
    watcher.shutdown()
  }

  // -------------------------------------------------------------------------
  // awaitStopSignal() — query stops naturally (shutdown called, file intact)
  // -------------------------------------------------------------------------

  test("awaitStopSignal returns false when shutdown() is called before file is deleted") {
    val watcher = new FileSystemWatcher(FileSystemStopConfig(testDir), jobName)
    watcher.start()
    val signal = watcher.awaitStopSignal()

    new Thread(() => {
      Thread.sleep(300)
      watcher.shutdown()  // simulates query terminating — file still present
    }).start()

    val result = Await.result(signal, 5.seconds)
    assert(!result)                       // false: stop signal was NOT received
    assert(new File(markerPath).exists()) // file still present
  }

  // -------------------------------------------------------------------------
  // shutdown()
  // -------------------------------------------------------------------------

  test("shutdown() is a no-op before start and does not throw") {
    val watcher = new FileSystemWatcher(FileSystemStopConfig(testDir), jobName)
    watcher.shutdown()
  }
}