import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap}
import akka.stream.scaladsl._
import akka.util.ByteString


object Main extends App {
  implicit val system = ActorSystem("MyAkkaActor")
  implicit val materializer = ActorMaterializer()
  implicit val dipatcher = system.dispatcher

  val path = Paths.get("test.csv")
  //val path = Paths.get("test_300000.csv")
  val source = FileIO.fromPath(path)

  val acc_empty = ("", 0)
  val grp_col = "state"

  source
    .via(CsvParsing.lineScanner())
    .via(CsvToMap.withHeaders("firstname", "lastname", "gender", "birthday", "addr1", "addr2", "addr3", "state", "email", "zip", "tel", "attr", "regdate"))
    .map(_.map(r => (r._1, r._2.utf8String)))
    .groupBy(2000, r => r(grp_col))
      .fold(acc_empty) { (acc: (String, Int), rec: Map[String, String]) =>
        val cnt = acc._2 + 1
        // println(rec(grp_col))
        (rec(grp_col), cnt)
      }.mergeSubstreams
    .map { case (groupname, count) => s"$groupname,$count" }
    .runWith(Sink.foreach(println))
    .onComplete(done ⇒ {
      println(done)
      system.terminate()
    })
    // .runForeach(println)

}
