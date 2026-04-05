package io.github.stopstreaming.extensions

import org.apache.spark.sql.streaming.StreamingQuery
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.util.{Failure, Success}
import io.github.stopstreaming.extensions.conf._
import io.github.stopstreaming.extensions.fs.FileSystemWatcher
import io.github.stopstreaming.extensions.rest.RestWatcher

object StreamingQueryOps {

  implicit class StreamingQueryOps(val self: StreamingQuery) extends AnyVal {

    /**
     * Awaits an external termination signal using the backend specified in `config`.
     *
     * - REST backend:        blocks until POST <stopPath>/<query.id> is received on the driver
     * - FileSystem backend:  blocks until stopDir/<query.id> marker file is deleted
     *
     * The job identifier is always derived from `self.id` — it must not be
     * included in the config. This ensures the watcher always targets the
     * correct query without any risk of misconfiguration.
     *
     * Config holds only connection/location parameters:
     * {{{
     *   val config = RestStopConfig(port = 8558)
     *   query.awaitExternalTermination(config)
     *   // → registers POST /stop/<query.id>
     * }}}
     *
     * @param config backend configuration (RestStopConfig or FileSystemStopConfig)
     */
    def awaitExternalTermination(config: StopConfig): Unit = {

      val jobId = self.id.toString

      val watcher: StopSignalWatcher = config match {
        case c: RestStopConfig       => new RestWatcher(c, jobId)
        case c: FileSystemStopConfig => new FileSystemWatcher(c, jobId, () => self.isActive)
      }

      watcher.start()

      val stopFuture: Future[Boolean] = watcher.awaitStopSignal().map { stopped =>
        if (stopped) self.stop()
        stopped
      }

      stopFuture.onComplete {
        case Success(false) => println("Watcher: query stopped before a stop signal was received.")
        case Failure(t)     => println(s"Watcher error: ${t.getMessage}")
        case _              =>
      }

      self.awaitTermination()
      watcher.shutdown()
    }
  }
}
