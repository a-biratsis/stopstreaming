package io.github.stopstreaming.extensions.fs

import io.github.stopstreaming.extensions.StopSignalWatcher
import io.github.stopstreaming.extensions.conf.{FileSystemStopConfig, FsType}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.{ExecutionContext, Future}

/**
 * FileSystem backend for external stream termination.
 *
 * On `start()`, creates a marker file at `config.stopDir/<jobName>`.
 * A background thread polls for the file's existence.
 * When the file is deleted externally the Future resolves with `true`
 * and the caller stops the streaming query.
 *
 * Uses `StreamingQueryListener` to detect when the query terminates for any
 * reason other than a stop signal, replacing the `isQueryActive` polling lambda.
 *
 * The `jobName` is NOT part of [[FileSystemStopConfig]] — it is derived from
 * `StreamingQuery.id` by [[io.github.stopstreaming.extensions.StreamingQueryOps]]
 * and injected here at construction time.
 *
 * @param config   FileSystemStopConfig with directory and FS type
 * @param jobName  streaming query ID, supplied by StreamingQueryOps
 * @param spark    SparkSession used to register the termination listener;
 *                 may be null in unit tests (listener is skipped)
 */
class FileSystemWatcher(
  config: FileSystemStopConfig,
  jobName: String,
  spark: SparkSession = null
)(implicit ec: ExecutionContext) extends StopSignalWatcher {

  private val fsWrapper: FileSystemWrapper = config.fsType match {
    case FsType.DBFS            => new DbfsWrapper(config.stopDir, jobName)
    case FsType.LocalFileSystem => new LocalFileSystemWrapper(config.stopDir, jobName)
  }

  @volatile private var running: Boolean = false

  private val listener = new StreamingQueryListener {
    override def onQueryStarted(e: QueryStartedEvent): Unit = {}
    override def onQueryProgress(e: QueryProgressEvent): Unit = {}
    override def onQueryTerminated(e: QueryTerminatedEvent): Unit =
      if (e.id.toString == jobName) running = false
  }

  override def start(): Unit = {
    running = true
    if (spark != null) spark.streams.addListener(listener)
    if (!fsWrapper.targetFileExists()) fsWrapper.createTargetFile("running")
  }

  /**
   * Polls every 10–100 ms until either:
   *  - the marker file is deleted        → returns true  (stop signal received)
   *  - the query terminates (via listener or shutdown()) → returns false
   */
  override def awaitStopSignal(): Future[Boolean] = Future {
    while (running && fsWrapper.targetFileExists()) {
      val sleep = ThreadLocalRandom.current().nextLong(10, 101)
      Thread.sleep(sleep)
    }
    !fsWrapper.targetFileExists()
  }

  override def shutdown(): Unit = {
    running = false
    if (spark != null) spark.streams.removeListener(listener)
  }
}