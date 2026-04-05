package io.github.stopstreaming.extensions.conf

import org.scalatest.funsuite.AnyFunSuite

class StopConfigLoaderTest extends AnyFunSuite {

  test("loads RestStopConfig from HOCON file") {
    val config = StopConfigLoader.load("test-rest.conf")

    config match {
      case r: RestStopConfig =>
        assert(r.host     == "127.0.0.1")
        assert(r.port     == 9999)
        assert(r.stopPath == "/test-stop")
      case other =>
        fail(s"Expected RestStopConfig, got: $other")
    }
  }

  test("loads FileSystemStopConfig from HOCON file") {
    val config = StopConfigLoader.load("test-filesystem.conf")

    config match {
      case f: FileSystemStopConfig =>
        assert(f.stopDir == "/tmp/test-stop")
        assert(f.fsType  == FsType.LocalFileSystem)
      case other =>
        fail(s"Expected FileSystemStopConfig, got: $other")
    }
  }

  test("throws IllegalArgumentException for unknown backend") {
    assertThrows[IllegalArgumentException] {
      StopConfigLoader.load("test-invalid.conf")
    }
  }

  test("returned config is a StopConfig instance") {
    assert(StopConfigLoader.load("test-rest.conf").isInstanceOf[StopConfig])
    assert(StopConfigLoader.load("test-filesystem.conf").isInstanceOf[StopConfig])
  }
}
