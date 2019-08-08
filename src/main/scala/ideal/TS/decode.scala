package ideal.TS

import java.nio.charset.{Charset, StandardCharsets}

import ideal.constants.Constants
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.spark.input.PortableDataStream
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.util.Try

/**
  * Created by zhangxiaofan on 2019/8/7.
  */
object decode {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().
      appName(this.getClass.getSimpleName)
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()
    import spark.implicits._
    val path = args(0)
    spark.sparkContext.binaryFiles(path)
      .flatMapValues(x => extractFiles(x).toOption)
      .mapValues(_.map(decode()))
      .map(_._2)
      .flatMap(x => x)
      .flatMap { x => x.split("\n") }
      .toDF().show()
    spark.stop()
  }

  def extractFiles(ps: PortableDataStream, n: Int = 1024) = Try {
    val tar = new TarArchiveInputStream(new GzipCompressorInputStream(ps.open))
    Stream.continually(Option(tar.getNextTarEntry))
      // Read until next exntry is null
      .takeWhile(_.isDefined)
      // flatten
      .flatMap(x => x)
      // Drop directories
      .filter(!_.isDirectory)
      .map(e => {
        Stream.continually {
          // Read n bytes
          val buffer = Array.fill[Byte](n)(-1)
          val i = tar.read(buffer, 0, n)
          (i, buffer.take(i))}
          // Take as long as we've read something
          .takeWhile(_._1 > 0)
          .map(_._2)
          .flatten
          .toArray})
      .toArray
  }
  def decode(charset: Charset = StandardCharsets.UTF_8)(bytes: Array[Byte]) = new String(bytes, StandardCharsets.UTF_8)

}
