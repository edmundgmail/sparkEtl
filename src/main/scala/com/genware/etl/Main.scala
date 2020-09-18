package com.genware.etl

import cats.effect.{ExitCode, IO, IOApp}
import com.genware.etl.common.{AppLogger, Environment}
import org.apache.log4j.Logger

object Main extends IOApp with Environment with AppLogger{

  override def run(args: List[String]): IO[ExitCode] = {
    val r = for{
      config <- parseParam(args)
      sparkSession <- initSparkSession(config)
      ctx <- setup(config, sparkSession)
    }yield (ctx.execute[IO])

    r.map{ p =>
      p.handleErrorWith {
        case e => IO(appLogger.error(e.getMessage))
      }.map(_ => ExitCode.Error)
    }.getOrElse(IO{
      appLogger.error(r.left.get.info)
      ExitCode.Error
    })
  }
}