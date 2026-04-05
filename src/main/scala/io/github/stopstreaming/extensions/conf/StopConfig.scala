package io.github.stopstreaming.extensions.conf

/**
 * Sealed marker trait for all backend configurations.
 * All subtypes must live in this file (Scala sealed constraint).
 *
 * Configs hold only connection/location parameters.
 * The job identifier is always derived from the StreamingQuery at runtime:
 *
 *   val config = RestStopConfig(port = 8558)
 *   query.awaitExternalTermination(config)   // jobId = query.id.toString internally
 */
sealed trait StopConfig

// ---------------------------------------------------------------------------
// REST backend
// ---------------------------------------------------------------------------

/**
 * Configuration for the REST backend.
 *
 * The driver starts an HTTP server on `host:port` and registers a context
 * at `stopPath/<query.id>`. The job ID is derived automatically from the
 * StreamingQuery — it must not be supplied here.
 *
 * @param host     network interface to bind (default: all interfaces)
 * @param port     HTTP port (default: 8558)
 * @param stopPath base URL path (default: /stop); effective path = stopPath/<query.id>
 */
case class RestStopConfig(
  host: String     = "0.0.0.0",
  port: Int        = 8558,
  stopPath: String = "/stop"
) extends StopConfig

// ---------------------------------------------------------------------------
// FileSystem backend
// ---------------------------------------------------------------------------

/** Selects the underlying file-system implementation. */
object FsType extends Enumeration {
  val LocalFileSystem, DBFS = Value
}

/**
 * Configuration for the FileSystem backend.
 *
 * The driver polls `stopDir` for a marker file named after the query ID,
 * which is derived automatically from the StreamingQuery at runtime.
 *
 * @param stopDir directory (local or DBFS) that holds the marker file
 * @param fsType  LocalFileSystem (default) or DBFS
 */
case class FileSystemStopConfig(
  stopDir: String,
  fsType: FsType.Value = FsType.LocalFileSystem
) extends StopConfig
