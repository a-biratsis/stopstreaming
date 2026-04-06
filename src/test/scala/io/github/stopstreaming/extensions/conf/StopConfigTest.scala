package io.github.stopstreaming.extensions.conf

import org.scalatest.funsuite.AnyFunSuite

class StopConfigTest extends AnyFunSuite {

  // -------------------------------------------------------------------------
  // RestStopConfig
  // -------------------------------------------------------------------------

  test("RestStopConfig uses correct defaults") {
    val c = RestStopConfig()
    assert(c.host     == "0.0.0.0")
    assert(c.port     == 8558)
    assert(c.stopPath == "/stop")
  }

  test("RestStopConfig accepts custom values") {
    val c = RestStopConfig(host = "127.0.0.1", port = 9000, stopPath = "/terminate")
    assert(c.host     == "127.0.0.1")
    assert(c.port     == 9000)
    assert(c.stopPath == "/terminate")
  }

  // -------------------------------------------------------------------------
  // FileSystemStopConfig
  // -------------------------------------------------------------------------

  test("FileSystemStopConfig defaults to LocalFileSystem") {
    val c = FileSystemStopConfig(stopDir = "/tmp/stop")
    assert(c.fsType == FsType.LocalFileSystem)
  }

  test("FileSystemStopConfig accepts DBFS fsType") {
    val c = FileSystemStopConfig(stopDir = "/dbfs/stop", fsType = FsType.DBFS)
    assert(c.fsType == FsType.DBFS)
  }

  test("FileSystemStopConfig stores stopDir correctly") {
    val c = FileSystemStopConfig(stopDir = "/tmp/mydir")
    assert(c.stopDir == "/tmp/mydir")
  }

  // -------------------------------------------------------------------------
  // StopConfig sealed trait — pattern matching exhaustiveness
  // -------------------------------------------------------------------------

  test("pattern match covers all StopConfig subtypes") {
    def describe(c: StopConfig): String = c match {
      case _: RestStopConfig       => "rest"
      case _: FileSystemStopConfig => "filesystem"
    }

    assert(describe(RestStopConfig())                          == "rest")
    assert(describe(FileSystemStopConfig(stopDir = "/tmp"))    == "filesystem")
  }

  // -------------------------------------------------------------------------
  // FsType enum
  // -------------------------------------------------------------------------

  test("FsType contains LocalFileSystem and DBFS values") {
    assert(FsType.values.contains(FsType.LocalFileSystem))
    assert(FsType.values.contains(FsType.DBFS))
  }
}
