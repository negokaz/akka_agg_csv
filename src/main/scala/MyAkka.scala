import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, OverflowStrategy}
import akka.stream.scaladsl._
import org.apache.commons.lang3.StringUtils

import scala.collection.mutable.AnyRefMap
import scala.util.Success


object Main extends App {
  implicit val system = ActorSystem("MyAkkaActor")
  val materializerSettings =
    ActorMaterializerSettings(system)
      .withDispatcher("csv-aggregate-dispatcher") // 専用の dispatcher（スレッドプール）を用意
  implicit val materializer = ActorMaterializer(materializerSettings)
  implicit val dipatcher = system.dispatcher


  // val path = if (params.isEmpty) "../fukuokaex/test_12_000_000.csv" else params(0)
  //val path = "../fukuokaex/test_3_000_000.csv"
  val path = "./test_3_000_000.csv"

  /** 集計処理を何並列で処理するか */
  val parallelism = 4

  /** CSVの何列目に LastName があるか */
  val columnNumberOfLastName = 2

  val fileSource = scala.io.Source.fromFile(path, enc = "utf-8")
  val source = Source.fromIterator(() => fileSource.getLines())

  val partitionedSource =
    source
      .map(rec => StringUtils.split(rec, ",", columnNumberOfLastName + 1)(columnNumberOfLastName - 1)) // String#split は正規表現を用いるため効率が悪い
      .toMat(PartitionHub.sink(
        partitioner = (size, rec) => Math.abs(rec.hashCode()) % size,
        startAfterNrOfConsumers = parallelism))(Keep.right).run()

  val start = System.currentTimeMillis()
  Source((1 to parallelism))
    .flatMapMerge(parallelism, { i =>
      partitionedSource
        .fold(AnyRefMap.empty[String, Int]) { (acc, rec: String) =>
          // 効率が良い AnyRefMap を使う
          acc.updated(rec, acc.getOrElse(rec, 0) + 1)
        }
    })
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
      case e =>
        println(e)
        fileSource.close()
        system.terminate()
    }
}
