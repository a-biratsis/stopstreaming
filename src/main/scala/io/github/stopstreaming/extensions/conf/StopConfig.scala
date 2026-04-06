package io.github.stopstreaming.extensions.conf

/**
 * Sealed marker trait for all backend configurations.
 *
 * Configs hold only connection/location parameters.
 * The job identifier is always derived from the StreamingQuery at runtime:
 *
 *   val config = RestStopConfig(port = 8558)
 *   query.awaitExternalTermination(config)   // jobId = query.id.toString internally
 */
sealed trait StopConfig

/**
 * Configuration for the REST backend.
 * The driver starts a JDK HttpServer bound to `config.port` and registers a context at `config.stopPath/<queryId>`.
 * A POST request to that path signals the streaming query to stop gracefully.
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


/** Selects the underlying file-system implementation. */
object FsType extends Enumeration {
  val LocalFileSystem, DBFS = Value
}

/**
 * Configuration for the FileSystem backend. 
 * The driver creates a marker file at `config.stopDir/<queryId>`. A background thread polls for the file's existence.
 * When the file is deleted externally the Future resolves with `true` and the caller stops the streaming query.
 *
 * @param stopDir directory (local or DBFS) that holds the marker file
 * @param fsType  LocalFileSystem (default) or DBFS
 */
case class FileSystemStopConfig(
  stopDir: String,
  fsType: FsType.Value = FsType.LocalFileSystem
) extends StopConfig
