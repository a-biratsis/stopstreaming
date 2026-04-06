package io.github.stopstreaming.extensions.fs

/**
  * Abstraction over the underlying file system used by FileSystemWatcher.
  * The driver creates a marker file at `stopDir/<queryId>`. A background thread polls for the file's existence.
  * When the file is deleted externally the Future resolves with `true` and the caller stops the streaming query.
  * Example usage:
  * {{{
  *   val config = FileSystemStopConfig(
  *     stopDir = "/tmp/stopstreaming",
  *     fsType = FsType.LocalFileSystem
  *   )
  *   query.awaitExternalTermination(config)
  * }}}
  * 
  * Note: Requires appropriate permissions to read/write in `stopDir`. In unit tests, the `targetFileExists` and `createTargetFile` methods can be mocked to simulate file system behavior without actual file operations.
  * @param stopDir directory that holds the marker file
  * @param targetFile name of the marker file (derived from query ID at runtime)
  */

trait FileSystemWrapper {
  val stopDir: String
  val targetFile: String
  val targetPath = s"${stopDir}/${targetFile}"

  /** Checks wherether the targetFile exists or not in stopDir.
    *
    * @return true if the file is present in stopDir false otherwise.
   */
  def targetFileExists() : Boolean

  /** Creates the targetFile into stopDir.
    *
    *  @param content the string to insert.
    */
  def createTargetFile(content: String) : Unit
}
