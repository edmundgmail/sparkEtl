package com.genware.etl.sources
import cats.effect._
import cats.implicits._
import com.genware.etl.common.{ContextExecutor, Utils}
import com.genware.etl.models.{ConfigParam, ErrorInfo}

case class HdfsLoader(path: String, outputAlias: Option[String], outputDf: Option[String], format: String, options: List[(String, String)], columns: String) extends DataSource {
  override def applyTemplate(config: ConfigParam): HdfsLoader = this

  override def load(context: ContextExecutor): Unit = {
    val df = options.foldLeft(context.spark.read.format(format))((z, b) => z.option(b._1, b._2)).load(path)
    val df1 = columns match {
      case "*" | "" => df
      case _ => {
        val columnsList = columns.split(",").toList
        df.select(columnsList.head, columnsList.tail: _*)
      }
    }



  }
}

object HdfsLoader {
  def apply(path: Option[Any], outputAlias: Option[Any], outputDf: Option[Any], format: Option[Any], options: Option[Any], columns: Option[Any]): Either[ErrorInfo, HdfsLoader] =
    (path,  format, columns) match {
      case (Some(p), Some(f), Some(c)) => Utils.toListMapWithOption(options).map(oo => new HdfsLoader(p.toString, outputAlias.map(_.toString), outputDf.map(_.toString), f.toString, oo, c.toString))
      case _ => Left(ErrorInfo("Can't construct HDFS loader"))
    }
}