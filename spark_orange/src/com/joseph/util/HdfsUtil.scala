package com.joseph.util

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.hadoop.fs.FileUtil
import java.io.PrintWriter

class HdfsUtil {
  
  /**
   * 写入到文件
   * line:内容
   * pathStr:路径
   */
  def writeText(line: String, pathStr: String) {
    val fs = FileSystem.get(new Configuration())
    val writer = new PrintWriter(fs.create(new Path(pathStr)))
    writer.println(line)
    writer.close()
  }
  
  /**
   * 删除hdfs的目录或者文件
   * pathStr:路径
   */
  def del(pathStr: String) {
    val fs = FileSystem.get(new Configuration())
    val path = new Path(pathStr)
    if (fs.exists(path)) {
      fs.delete(path)
    }
  }
  
  /**
   * 递归目录下所有的文件
   * path:路径
   * flag:过滤的标识
   */
  def subdirs2(path: Path, flag: String): Iterator[Path] = {
    val hadoopConfiguration = SparkHadoopUtil.get.newConfiguration()
    val fs = path.getFileSystem(hadoopConfiguration)
    val listpath = FileUtil.stat2Paths(fs.listStatus(path))
    val d = listpath.filter { line => fs.isDirectory(line) }
    val f = listpath.filter { line => !"".equals(line.toString()) && fs.isFile(line) && line.toString().contains(flag) }.toIterator
    f ++ d.toIterator.flatMap(subdirs2(_, flag))
  }
  
  /**
   * 删除hdfs的目录或者文件
   * pathStr:路径
   */
  def deletePath(pathStr: String) {
    val hadoopConfiguration = SparkHadoopUtil.get.newConfiguration()
    val path = new Path(pathStr)
    val fs = path.getFileSystem(hadoopConfiguration)

    fs.isDirectory(path)
    if (fs.exists(path)) {
      fs.delete(path)
    }
  }
  
  /**
   * 获得hdfs路径
   */
  def hdfsPath(path:Path):String={
    val hadoopConfiguration = SparkHadoopUtil.get.newConfiguration()
    val fs = path.getFileSystem(hadoopConfiguration)
    fs.getUri.toString() + path.toString()
  }
}