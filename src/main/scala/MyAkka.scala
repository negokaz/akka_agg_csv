import akka.actor.{Actor, ActorSystem, Props}
import akka.stream.ActorMaterializer

import akka.stream.alpakka.csv.scaladsl.CsvParsing
import akka.stream.alpakka.csv.scaladsl.CsvToMap
import java.nio.file.Paths
import java.nio.charset.StandardCharsets

import akka.NotUsed

import akka.util.ByteString
import akka.stream.scaladsl._
import akka.stream.{ActorMaterializer}


object Main extends App {
  implicit val system = ActorSystem("MyAkkaActor")
  implicit val materializer = ActorMaterializer()

  val path = Paths.get("test.csv")
  //val path = Paths.get("test_300000.csv")
  val source = FileIO.fromPath(path)

  val acc_empty = ("", 0)
  val grp_col = "state"

  source
    .via(CsvParsing.lineScanner())
    .via(CsvToMap.withHeaders("firstname", "lastname", "gender", "birthday", "addr1", "addr2", "addr3", "state", "email", "zip", "tel", "attr", "regdate"))
    .via(Flow[Map[String, ByteString]].map(_.map(r => (r._1, r._2.utf8String))))
    .via(Flow[Map[String, String]].groupBy(2000, r => r(grp_col))
      .fold(acc_empty) { (acc: (String, Int), rec: Map[String, String]) =>
        val cnt = acc._2 + 1
        // println(rec(grp_col))
        (rec(grp_col), cnt)
      }.mergeSubstreams
    )
    .via(Flow[(String, Int)].map { case (groupname, count) => s"$groupname,$count" })
    .runWith(Sink.foreach(println))
    // .runForeach(println)

}
