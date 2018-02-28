import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, OverflowStrategy}
import akka.stream.scaladsl._
import org.apache.commons.lang3.StringUtils

import scala.collection.mutable.AnyRefMap
import scala.util.Success


object Main extends App {
  implicit val system = ActorSystem("MyAkkaActor")
  val materializerSettings = ActorMaterializerSettings(system).withDispatcher("csv-aggregate-dispatcher")
  implicit val materializer = ActorMaterializer(materializerSettings)
  implicit val dipatcher = system.dispatcher


  // val path = if (params.isEmpty) "../fukuokaex/test_12_000_000.csv" else params(0)
  //val path = "../fukuokaex/test_3_000_000.csv"
  val path = "./test_3_000_000.csv"
  val fileSource = scala.io.Source.fromFile(path, enc = "utf-8")
  val source = Source.fromIterator(() => fileSource.getLines())

  val groupSize = 30

  val indexOfLastName = 1

  val start = System.currentTimeMillis()
  source
    .map(rec => StringUtils.split(rec, ",", indexOfLastName + 2)(indexOfLastName)) // String#split は正規表現を用いるため効率が悪い
    .groupBy(groupSize, rec => Math.abs(rec.hashCode()) % groupSize) // String#hashCode を使って、名前ごとに一意の group にディスパッチされるようにする
    .fold(AnyRefMap.empty[String, Int]) { (acc, rec: String) =>
      // 効率が良い AnyRefMap を使う
      acc.updated(rec, acc.getOrElse(rec, 0) + 1)
    }
    .mergeSubstreams
    .runWith(Sink.fold(AnyRefMap.empty[String, Int]) { (acc, rec: AnyRefMap[String, Int]) =>
      // LastName の hashCode でグループ分けしたため、acc と rec で同じキーの要素は存在しない
      acc ++ rec
    })
    .onComplete {
      case Success(resultMap) ⇒
        val msec = (System.currentTimeMillis - start)
        resultMap.toSeq.sortWith(_._2 > _._2).take(10).foreach(println)
        println(msec + "msec")
        fileSource.close()
        system.terminate()
      case _ =>
        fileSource.close()
        system.terminate()
    }
}
