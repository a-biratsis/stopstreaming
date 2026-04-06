package io.github.stopstreaming.extensions

import scala.concurrent.Future

/**
 * Common trait for all stop-signal backends.
 * Each backend monitors a different signal source and resolves
 * the returned Future with `true` when a termination signal is received.
 *
 * Implementations must be non-blocking: the watcher logic runs
 * inside a Future so it never occupies the main streaming thread.
 */
trait StopSignalWatcher {

  /** Starts the underlying watcher (e.g. opens a server socket, begins polling). */
  def start(): Unit

  /**
   * Returns a Future that completes with `true` when the stop signal
   * is received, or `false` / Failure if the watch could not succeed.
   *
   * Must be called after `start()`.
   */
  def awaitStopSignal(): Future[Boolean]

  /** Releases any resources held by the watcher (sockets, threads, etc.). */
  def shutdown(): Unit
}
