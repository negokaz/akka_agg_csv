import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap}
import akka.stream.scaladsl._
import akka.util.ByteString

import scala.util.Success


object Main extends App {
  implicit val system = ActorSystem("MyAkkaActor")
  implicit val materializer = ActorMaterializer()
  implicit val dipatcher = system.dispatcher


  // val path = if (params.isEmpty) "../fukuokaex/test_12_000_000.csv" else params(0)
  //val path = "../fukuokaex/test_3_000_000.csv"
  val path = "./test_3_000_000.csv"
  val source = FileIO.fromPath(Paths.get(path))

  val acc_empty = Map.empty[String, Int]
  var resultMap = Map.empty[String, Int]
  val grp_col = "lastname"

  val groupSize = 8

  val indexOfLastName = 1

  /** hashCode でグループ分け */
  def extractGroupId(elem: ByteString) = {
    Math.abs(elem.hashCode()) % groupSize
  }

  val start = System.currentTimeMillis()
  source
    .via(Framing.delimiter(ByteString('\n'), maximumFrameLength = Int.MaxValue))
    .async // 非同期でファイルを読み込む
    .groupBy(groupSize, extractGroupId) // Group ごとに並列処理
    .buffer(10, OverflowStrategy.backpressure) // 下流に速度差がある場合に back pressure がかかるのを防止
    .map(rec => rec.utf8String.split(',')(indexOfLastName))
    .async // 下流が他のステップと比べて重そうなので
    .fold(acc_empty) { (acc: Map[String, Int], rec: String) =>
      acc + (rec -> (acc.getOrElse(rec, 0) + 1))
    }
    .mergeSubstreams
    .runWith(Sink.fold(acc_empty) { (acc: Map[String, Int], rec: Map[String, Int]) =>
      acc ++ rec.map { case (k, v) => k -> (v + acc.getOrElse(k, 0)) }
    })
    .onComplete {
      case Success(resultMap) ⇒
        val msec = (System.currentTimeMillis - start)
        resultMap.toSeq.sortWith(_._2 > _._2).take(10).foreach(println)
        println(msec + "msec")
        system.terminate()
      case _ =>
        system.terminate()
    }
}
