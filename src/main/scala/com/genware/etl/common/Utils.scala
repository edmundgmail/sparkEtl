package com.genware.etl.common

import com.genware.etl.models.ErrorInfo

import scala.collection.JavaConverters._
import scala.collection.Map
import scala.reflect.{ClassTag, _}


object Utils {
  def toOptionBoolean(a: Option[Any]) = a match {
    case Some(x) if (x.toString.equalsIgnoreCase("true") || x.toString.equalsIgnoreCase("yes")) => true
    case _ => false
  }

  def translateEsConf(config: Map[String ,_]): Option[List[(String, String)]] = {
    def getPass(pass: String) = config.get("encryption.code") match {
      case Some(_) => pass //TODO: add decrypt
      case _ => pass
    }

    for{
      user<-config.get("es.net.http.auth.user")
      pwd<-config.get("es.net.http.auth.pass")
    }yield config.flatMap {
      case ("encryption.code", v) => List()
      case ("es.net.http.auth.pass", v) => List(("es.net.http.auth.pass", getPass(pwd.toString)), ("", s"${user}:${getPass(pwd.toString)}"))
      case (k, v) => List((k, v.toString))
      case _ => List()
    }(collection.breakOut): List[(String, String)]
  }

  def toListMapWithOption(a: Option[Any]): Either[ErrorInfo, List[(String, String)]] = {
     a match {
      case Some(o) => Utils.toListMap(o)
      case _ => Right(List())
    }
  }

  def toListMap(a: Any): Either[ErrorInfo, List[(String, String)]] = {
     for{
         ta<-toT[List[Map[String, String]]](a)
     }yield ta.map(c=>(c.head._1, c.head._2))
    }

  def toScalaRecursively(a: Any): Any = a match {
    case al: java.util.ArrayList[_] => al.asScala.toList.map(toScalaRecursively)
    case hm: java.util.HashMap[_, _] => hm.asScala.foldLeft(Map[String, Any]()){
      (z, b) => b match {
        case (k: String, v) => z+(k->toScalaRecursively(v))
        case _ => z
      }
    }
    case _ => a
  }

  def toT[T: ClassTag](a: Any) : Either[ErrorInfo, T] = a match {
    case t: T => Right(t)
    case _ => Left(ErrorInfo((s"Can't cast ${a.toString} to class ${classTag[T]}")))
  }

  def toScalaTypeT[T: ClassTag](a: Any): Either[ErrorInfo, T] = toT(toScalaRecursively(a))
}
