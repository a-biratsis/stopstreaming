package io.github.stopstreaming.extensions.rest

import io.github.stopstreaming.extensions.StopSignalWatcher
import io.github.stopstreaming.extensions.conf.RestStopConfig
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import java.net.InetSocketAddress
import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future}

/**
 * REST backend for external stream termination.
 *
 * Starts a lightweight HTTP server on the Spark driver and registers a
 * context at `config.stopPath/<jobId>`.
 *
 * The `jobId` is NOT part of [[RestStopConfig]] — it is derived from
 * `StreamingQuery.id` by [[io.github.stopstreaming.extensions.StreamingQueryOps]]
 * and injected here at construction time.
 *
 * Multiple streaming queries can share the same port because each registers
 * a distinct path:
 *
 *   POST /stop/<id-of-A>  →  stops only query A
 *   POST /stop/<id-of-B>  →  stops only query B
 *
 * Requires JVM flag: --add-exports=jdk.httpserver/com.sun.net.httpserver=ALL-UNNAMED
 *
 * @param config RestStopConfig with host, port and stopPath
 * @param jobId  streaming query ID, supplied by StreamingQueryOps
 */
class RestWatcher(config: RestStopConfig, jobId: String)(implicit ec: ExecutionContext)
    extends StopSignalWatcher {

  /** Full HTTP context path: config.stopPath/jobId */
  val effectivePath: String = s"${config.stopPath}/$jobId"

  // Written by the HTTP handler thread; read by the polling loop thread.
  @volatile private var stopSignalReceived: Boolean = false

  // Set to false by shutdown() to break the poll loop when the query stops
  // for a reason other than a REST stop signal.
  @volatile private var running: Boolean = false

  @volatile private var server: HttpServer = _

  override def start(): Unit = {
    server = HttpServer.create(new InetSocketAddress(config.host, config.port), /*backlog=*/ 0)

    server.createContext(effectivePath, (exchange: HttpExchange) => {
      try {
        if (exchange.getRequestMethod.equalsIgnoreCase("POST")) {
          val bytes = "Stop signal received.".getBytes("UTF-8")
          exchange.sendResponseHeaders(200, bytes.length)
          val out = exchange.getResponseBody
          out.write(bytes)
          out.close()
          stopSignalReceived = true
        } else {
          exchange.sendResponseHeaders(405, -1)
          exchange.close()
        }
      } catch {
        case _: Exception => exchange.close()
      }
    })

    server.setExecutor(Executors.newSingleThreadExecutor())
    server.start()
    running = true
  }

  /**
   * Parks a background thread in a poll loop until either:
   *  - a POST to effectivePath is received  → returns true
   *  - shutdown() is called                 → returns false
   */
  override def awaitStopSignal(): Future[Boolean] = Future {
    while (running && !stopSignalReceived) {
      Thread.sleep(100)
    }
    stopSignalReceived
  }

  override def shutdown(): Unit = {
    running = false
    if (server != null) server.stop(0)
  }
}
