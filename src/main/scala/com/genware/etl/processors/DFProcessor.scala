package com.genware.etl.processors

import cats.effect.Sync
import com.genware.etl.common.{AppLogger, ContextExecutor, Utils}
import com.genware.etl.models.ErrorInfo
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.parallel.ParMap
import scala.reflect.runtime.universe.runtimeMirror
import scala.tools.reflect.ToolBox

case class DFProcessor(inputDf: List[String], text: String, outputDf: Option[String], outputAlias: Option[String]) extends DataProcessor with AppLogger {
  val tb = runtimeMirror(getClass.getClassLoader).mkToolBox()

  override def process[F[_]: Sync](context: ContextExecutor): F[Unit] = Sync[F].delay {

    val param = inputDf.map(d=>s"val ${d}: DataFrame = dfs.get(${'"'}${d}${'"'}).getOrElse(spark.emptyDataFrame)").mkString("\n")
    val program =
      s"""
        |import scala.collection.parallel.ParMap
        |import org.apache.spark.sql.{SparkSession, DataFrame}
        |def wrapper(p: (SparkSession, ParMap[String, DataFrame])) = {
        |val spark = p._1
        |val dfs = p._2
        |import spark.implicits._
        |${param}
        |${text}
        |}
        |wrapper _
      """.stripMargin

    val tree = tb.parse(program)
    val f = tb.compile(tree)
    val wrapper = f()
    val dfParsed = wrapper.asInstanceOf[((SparkSession, ParMap[String, DataFrame]))=>DataFrame]((context.spark, context.df.toMap))

    outputDf match {
      case Some(d) => context.df.put(d.toString, dfParsed)
      case _ =>
    }

    outputAlias match {
      case Some(a) => dfParsed.createOrReplaceGlobalTempView(a.toString)
      case _ =>
    }
  }
}

object DFProcessor {
  def apply(inputDf: Option[Any], text: Option[Any], outputDf: Option[Any], outputAlias: Option[Any]): Either[ErrorInfo, DFProcessor] = text match {
    case Some(s) => {
      val dataframes = inputDf.map(s=>Utils.toT[List[String]](s)).getOrElse(Right(List.empty[String]))
      dataframes.map(df=>new DFProcessor(df, s.toString, outputDf.map(_.toString), outputAlias.map(_.toString)))
    }
    case _ => Left(ErrorInfo("text is a must for dfprocessor"))
  }
}