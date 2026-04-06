package io.github.stopstreaming.extensions.fs

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

/**
  * DBFS implementation of FileSystemWrapper.
  * The driver creates a marker file at `stopDir/<queryId>`. A background thread polls for the file's existence.
  * When the file is deleted externally the Future resolves with `true` and the caller stops the streaming query.
  * Note: DBFS paths must start with "dbfs:/". For example, if `stopDir` is "dbfs:/tmp/stopstreaming" and the query ID is "abc123", the marker file will be "dbfs:/tmp/stopstreaming/abc123". 
  * 
  * Example usage:
  * {{{
  *   val config = FileSystemStopConfig(
  *     stopDir = "dbfs:/tmp/stopstreaming",
  *     fsType = FsType.DBFS
  *   )
  *   query.awaitExternalTermination(config)
  * }}}
  * 
  * Note: Requires the `dbutils-api` dependency and a Databricks environment to run. In unit tests, the `targetFileExists` and `createTargetFile` methods can be mocked to simulate DBFS behavior without actual file operations.
  * @param stopDir directory in DBFS that holds the marker file
  * @param targetFile name of the marker file (derived from query ID at runtime) 
 */
class DbfsWrapper(val stopDir: String, val targetFile: String) extends FileSystemWrapper {
  override def targetFileExists(): Boolean = {
    try {
      dbutils.fs.ls(targetPath).size > 0
    }
    catch {
      case _: java.io.FileNotFoundException => false
    }
  }

  override def createTargetFile(content: String): Unit = {
    dbutils.fs.put(targetPath, content)
  }
}
