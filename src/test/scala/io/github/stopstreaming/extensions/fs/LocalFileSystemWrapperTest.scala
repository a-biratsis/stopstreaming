package io.github.stopstreaming.extensions.fs

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach
import java.io.File

class LocalFileSystemWrapperTest extends AnyFunSuite with BeforeAndAfterEach {

  private val testDir  = "/tmp/local_fs_wrapper_test"
  private val fileName = "marker.txt"

  override def beforeEach(): Unit = {
    val dir = new File(testDir)
    if (!dir.exists()) dir.mkdirs()
  }

  override def afterEach(): Unit = {
    val f = new File(s"$testDir/$fileName")
    if (f.exists()) f.delete()
  }

  // -------------------------------------------------------------------------
  // targetPath
  // -------------------------------------------------------------------------

  test("targetPath is composed of stopDir and targetFile") {
    val wrapper = new LocalFileSystemWrapper(testDir, fileName)
    assert(wrapper.targetPath == s"$testDir/$fileName")
  }

  // -------------------------------------------------------------------------
  // targetFileExists
  // -------------------------------------------------------------------------

  test("targetFileExists returns false when file is absent") {
    val wrapper = new LocalFileSystemWrapper(testDir, fileName)
    assert(!wrapper.targetFileExists())
  }

  test("targetFileExists returns true after createTargetFile") {
    val wrapper = new LocalFileSystemWrapper(testDir, fileName)
    wrapper.createTargetFile("running")
    assert(wrapper.targetFileExists())
  }

  // -------------------------------------------------------------------------
  // createTargetFile
  // -------------------------------------------------------------------------

  test("createTargetFile writes content to the file") {
    val wrapper = new LocalFileSystemWrapper(testDir, fileName)
    wrapper.createTargetFile("hello")
    val content = scala.io.Source.fromFile(s"$testDir/$fileName").mkString
    assert(content == "hello")
  }

  test("createTargetFile overwrites existing content") {
    val wrapper = new LocalFileSystemWrapper(testDir, fileName)
    wrapper.createTargetFile("first")
    wrapper.createTargetFile("second")
    val content = scala.io.Source.fromFile(s"$testDir/$fileName").mkString
    assert(content == "second")
  }

  test("targetFileExists returns false after file is manually deleted") {
    val wrapper = new LocalFileSystemWrapper(testDir, fileName)
    wrapper.createTargetFile("running")
    new File(s"$testDir/$fileName").delete()
    assert(!wrapper.targetFileExists())
  }
}
