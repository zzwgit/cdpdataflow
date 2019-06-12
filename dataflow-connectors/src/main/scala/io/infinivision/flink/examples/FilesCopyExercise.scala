package io.infinivision.flink.examples

import java.net.URI
import java.nio.file.{Files, StandardCopyOption}

import org.apache.flink.core.fs.local.LocalFileSystem
import org.apache.flink.core.fs.{FileSystem, Path}
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem


object FilesCopyExercise {
  def main(args: Array[String]): Unit = {
    val keyTabPath = "hdfs:///user/infinivision_flink_user/infinivision_flink_user.keytab"
//    val path = "hdfs://172.19.0.16:9000/a.txt"
//    val fs = FileSystem.get(URI.create(path))
//    if (fs.isInstanceOf[HadoopFileSystem]) {
//      println("hadoop file system")
//    }

//    FileSystem.get(URI.create(path)) match {
//      case fs: HadoopFileSystem =>
//        println("hadoop file system")
//      case _ =>
//    }

    val fs = FileSystem.get(URI.create(keyTabPath))
    println(s"fs: $fs")
    if (fs.isDistributedFS) {
      println("distribute")
    }
      //    val hadoopFs = fs.asInstanceOf[HadoopFileSystem]
//    val inputStream = hadoopFs.open(new Path(path))
//    val targetPath = Files.createTempFile("Flink-AsyncHBase-", ".keytab")
//    Files.copy(inputStream, targetPath, StandardCopyOption.REPLACE_EXISTING)
//
//    println(s"target Path: ${targetPath.toUri.getPath}")
//
//    targetPath.toFile.deleteOnExit()
//    inputStream.close()
  }

}
