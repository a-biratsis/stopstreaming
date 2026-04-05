package io.github.stopstreaming.extensions.fs

import io.github.stopstreaming.extensions.StopSignalWatcher
import io.github.stopstreaming.extensions.conf.{FileSystemStopConfig, FsType}
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
 * The `jobName` is NOT part of [[FileSystemStopConfig]] — it is derived from
 * `StreamingQuery.id` by [[io.github.stopstreaming.extensions.StreamingQueryOps]]
 * and injected here at construction time.
 *
 * @param config        FileSystemStopConfig with directory and FS type
 * @param jobName       streaming query ID, supplied by StreamingQueryOps
 * @param isQueryActive predicate returning false once the query has stopped
 *                      (used to exit the poll loop if the query stops for another reason)
 */
class FileSystemWatcher(
  config: FileSystemStopConfig,
  jobName: String,
  isQueryActive: () => Boolean
)(implicit ec: ExecutionContext) extends StopSignalWatcher {

  private val fsWrapper: FileSystemWrapper = config.fsType match {
    case FsType.DBFS            => new DbfsWrapper(config.stopDir, jobName)
    case FsType.LocalFileSystem => new LocalFileSystemWrapper(config.stopDir, jobName)
  }

  override def start(): Unit = {
    if (!fsWrapper.targetFileExists())
      fsWrapper.createTargetFile("running")
  }

  /**
   * Polls every 10–100 ms until either:
   *  - the marker file is deleted  → returns true  (stop signal received)
   *  - the query stops on its own  → returns false (no stop signal)
   */
  override def awaitStopSignal(): Future[Boolean] = Future {
    while (isQueryActive() && fsWrapper.targetFileExists()) {
      val sleep = ThreadLocalRandom.current().nextLong(10, 101)
      Thread.sleep(sleep)
    }
    !fsWrapper.targetFileExists()
  }

  override def shutdown(): Unit = ()
}
