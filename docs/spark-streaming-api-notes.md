# Spark Streaming API — Useful Classes in `org.apache.spark.sql.streaming`

These are built-in Spark classes that can complement or replace custom implementations in this project.

---

## `StreamingQueryListener`

Event-driven hooks for the full query lifecycle. Register via `spark.streams.addListener(...)`.

```scala
spark.streams.addListener(new StreamingQueryListener {
  override def onQueryStarted(e: QueryStartedEvent): Unit = {}
  override def onQueryProgress(e: QueryProgressEvent): Unit = {}
  override def onQueryIdle(e: QueryIdleEvent): Unit = {}       // Spark 3.5+

  override def onQueryTerminated(e: QueryTerminatedEvent): Unit = {
    if (e.id == query.id) {
      // fires immediately when the query stops, for any reason
      // e.exception: Option[String] — present if stopped due to error
      watcher.shutdown()
    }
  }
})
```

**Relevance to this project:** replaces the `isQueryActive: () => Boolean` lambda passed to
`FileSystemWatcher`. Instead of polling `isActive` every 10–100 ms, `onQueryTerminated` fires
immediately when the query stops, breaking the poll loop with zero lag.

**Trade-off:** requires passing `SparkSession` into `FileSystemWatcher`.

---

## `StreamingQueryManager` (`spark.streams`)

Manages all streaming queries running in a `SparkSession`.

| Method | Description |
|---|---|
| `spark.streams.active` | `Array[StreamingQuery]` — all currently active queries |
| `spark.streams.get(id: UUID)` | Look up a specific query by its UUID |
| `spark.streams.awaitAnyTermination()` | Block until **any** query in the session stops |
| `spark.streams.awaitAnyTermination(timeoutMs)` | Same, with a timeout |
| `spark.streams.resetTerminated()` | Reset state so `awaitAnyTermination` can be called again |
| `spark.streams.addListener(l)` | Register a `StreamingQueryListener` |
| `spark.streams.removeListener(l)` | Deregister a listener |

**Relevance to this project:** `awaitAnyTermination` can serve as an alternative to calling
`query.awaitTermination()` directly — useful when managing multiple queries in the same session.

---

## Potential refactor: event-driven `FileSystemWatcher`

Current approach (polling):

```scala
while (isQueryActive() && fsWrapper.targetFileExists()) {
  Thread.sleep(ThreadLocalRandom.current().nextLong(10, 101))
}
```

Alternative using `StreamingQueryListener` (event-driven):

```scala
class FileSystemWatcher(
  config: FileSystemStopConfig,
  spark: SparkSession
)(implicit ec: ExecutionContext) extends StopSignalWatcher {

  @volatile private var queryTerminated = false

  private val listener = new StreamingQueryListener {
    def onQueryStarted(e: QueryStartedEvent): Unit = {}
    def onQueryProgress(e: QueryProgressEvent): Unit = {}
    def onQueryTerminated(e: QueryTerminatedEvent): Unit = {
      queryTerminated = true
    }
  }

  override def start(): Unit = {
    spark.streams.addListener(listener)
    if (!fsWrapper.targetFileExists()) fsWrapper.createTargetFile("running")
  }

  override def awaitStopSignal(): Future[Boolean] = Future {
    while (!queryTerminated && fsWrapper.targetFileExists()) {
      Thread.sleep(100)
    }
    !fsWrapper.targetFileExists()
  }

  override def shutdown(): Unit = spark.streams.removeListener(listener)
}
```
