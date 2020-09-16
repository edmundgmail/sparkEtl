package com.genware.etl

import cats.effect.{ExitCode, IO, IOApp}
import com.genware.etl.common.Environment

object Main extends IOApp with Environment{
  override def run(args: List[String]): IO[ExitCode] = {
    val r = for{
      config <- parseParam(args)
      sparkSession <- initSparkSession(config)
      ctx <- setup(config, sparkSession)
    }yield (ctx.execute[IO])

    r.map{ p =>
      p.handleErrorWith {
        case e => IO(e.printStackTrace)
      }.map(_ => ExitCode.Error)
    }.getOrElse(IO.pure(ExitCode.Error))
  }
}