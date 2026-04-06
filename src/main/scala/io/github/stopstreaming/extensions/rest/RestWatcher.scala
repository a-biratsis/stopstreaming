package io.github.stopstreaming.extensions.rest

import io.github.stopstreaming.extensions.StopSignalWatcher
import io.github.stopstreaming.extensions.conf.RestStopConfig
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import java.net.InetSocketAddress
import java.util.concurrent.Executors
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

/**
 * One HttpServer per (host, port) pair, shared across all RestWatcher instances
 * on the same driver. Each watcher registers its own context path on the shared
 * server; the server is stopped only when the last watcher releases it.
 */
private[rest] object SharedHttpServer {

  private case class Entry(server: HttpServer, var refCount: Int)
  private val registry = mutable.HashMap.empty[Int, Entry]

  def acquire(host: String, port: Int): HttpServer = synchronized {
    registry.get(port) match {
      case Some(e) =>
        e.refCount += 1
        e.server
      case None =>
        val s = HttpServer.create(new InetSocketAddress(host, port), /*backlog=*/ 0)
        s.setExecutor(Executors.newCachedThreadPool())
        s.start()
        registry(port) = Entry(s, 1)
        s
    }
  }

  def release(port: Int, contextPath: String): Unit = synchronized {
    registry.get(port).foreach { e =>
      try e.server.removeContext(contextPath)
      catch { case _: IllegalArgumentException => /* context already gone */ }
      e.refCount -= 1
      if (e.refCount <= 0) {
        e.server.stop(0)
        registry.remove(port)
      }
    }
  }
}

/**
 * REST backend for external stream termination.
 *
 * Registers a context at `config.stopPath/<jobId>` on a shared JDK HttpServer
 * bound to `config.port`. Multiple streaming queries on the same driver can
 * share the same port — each gets a distinct path:
 *
 *   POST /stop/<id-of-A>  →  stops only query A
 *   POST /stop/<id-of-B>  →  stops only query B
 *
 * Uses `StreamingQueryListener` to detect when the query terminates for any
 * reason other than a REST stop signal, replacing the `running` flag polling.
 *
 * The `jobId` is NOT part of [[RestStopConfig]] — it is derived from
 * `StreamingQuery.id` by [[io.github.stopstreaming.extensions.StreamingQueryOps]]
 * and injected here at construction time.
 *
 * @param config RestStopConfig with host, port and stopPath
 * @param jobId  streaming query ID, supplied by StreamingQueryOps
 * @param spark  SparkSession used to register the termination listener;
 *               may be null in unit tests (listener is skipped)
 */
class RestWatcher(config: RestStopConfig, jobId: String, spark: SparkSession = null)(implicit ec: ExecutionContext)
    extends StopSignalWatcher {

  /** Full HTTP context path: config.stopPath/jobId */
  val effectivePath: String = s"${config.stopPath}/$jobId"

  @volatile private var stopSignalReceived: Boolean = false
  @volatile private var running: Boolean = false

  private val listener = new StreamingQueryListener {
    override def onQueryStarted(e: QueryStartedEvent): Unit = {}
    override def onQueryProgress(e: QueryProgressEvent): Unit = {}
    override def onQueryTerminated(e: QueryTerminatedEvent): Unit =
      if (e.id.toString == jobId) running = false
  }

  override def start(): Unit = {
    val server = SharedHttpServer.acquire(config.host, config.port)

    server.createContext(effectivePath, (exchange: HttpExchange) => {
      try {
        if (exchange.getRequestMethod.equalsIgnoreCase("POST")) {
          val bytes = s"Stop signal received for job $jobId".getBytes("UTF-8")
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

    running = true
    if (spark != null) spark.streams.addListener(listener)
  }

  /**
   * Parks a background thread in a poll loop until either:
   *  - a POST to effectivePath is received        → returns true
   *  - query terminates (via listener or shutdown) → returns false
   */
  override def awaitStopSignal(): Future[Boolean] = Future {
    while (running && !stopSignalReceived) {
      Thread.sleep(100)
    }
    stopSignalReceived
  }

  override def shutdown(): Unit = {
    running = false
    if (spark != null) spark.streams.removeListener(listener)
    SharedHttpServer.release(config.port, effectivePath)
  }
}