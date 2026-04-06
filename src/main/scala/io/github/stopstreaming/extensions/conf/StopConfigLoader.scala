package io.github.stopstreaming.extensions.conf

import com.typesafe.config.ConfigFactory

/**
 * Loads a [[StopConfig]] from an HOCON config file on the classpath.
 *
 * Default resource: `application.conf` (src/main/resources/application.conf).
 *
 * Example application.conf:
 * {{{
 *   stopstreaming {
 *     backend = "rest"
 *     rest    { host = "0.0.0.0", port = 8558, stop-path = "/stop" }
 *   }
 * }}}
 *
 * Usage:
 * {{{
 *   val config = StopConfigLoader.load()
 *   query.awaitExternalTermination(config)
 * }}}
 */
object StopConfigLoader {

  def load(resourcePath: String = "application.conf"): StopConfig = {
    val root = ConfigFactory.load(resourcePath).getConfig("stopstreaming")

    root.getString("backend").toLowerCase match {

      case "rest" =>
        val r = root.getConfig("rest")
        RestStopConfig(
          host     = r.getString("host"),
          port     = r.getInt("port"),
          stopPath = r.getString("stop-path")
        )

      case "filesystem" =>
        val f = root.getConfig("filesystem")
        FileSystemStopConfig(
          stopDir = f.getString("stop-dir"),
          fsType  = f.getString("fs-type") match {
            case "DBFS" => FsType.DBFS
            case _      => FsType.LocalFileSystem
          }
        )

      case other =>
        throw new IllegalArgumentException(
          s"Unknown backend '$other' in $resourcePath. Supported values: rest, filesystem."
        )
    }
  }
}
