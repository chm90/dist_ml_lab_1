package se.kth.spark.lab1.task1

import se.kth.spark.lab1._

import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.ml.Pipeline
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
object Main {
  
    case class Song(year : Int,features : Array[Float]){
      val list_str = features.mkString(",")
      override def toString = s"Song(year = $year,features = [$list_str])"
      //This should be default implementation of toString str???!!!
    }
  
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("lab1").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._
    import sqlContext._

    val filePath = "src/main/resources/millionsong.txt"
    //val rawDF = ???

    val rdd = sc.textFile(filePath)

    //Step1: print the first 5 rows, what is the delimiter, number of features and the data types?

    //Step2: split each row into an array of features
    val recordsRdd = rdd.map(rec => rec.split(",").map(_.toFloat))
    //val tk = recordsRdd.take(10)
    //tk.foreach(rec =>{
    //  rec.foreach(print)
    //  println
    //})
    

    val songsRdd = recordsRdd.map(rec => Song(rec(0).toInt,rec.slice(1, 4)))
    
    songsRdd.take(10).foreach(println)
    
    //Step4: convert your rdd into a dataframe
   val songsDf = songsRdd.toDF()
   val n_rows =songsDf.count 
   println(s"rows = $n_rows")
   val filtered_songs_1 = songsDf.where($"year" >= 1998 && $"year" <= 2000)
   val filtered_songs_1_n = filtered_songs_1.count
   println(s"songs between 1998 inclusive and 2000 inclusive is $filtered_songs_1_n")
   val min_max = songsDf.agg(min($"year"),max($"year")).first()
   val min_val = min_max(0)
   val max_val = min_max(1)
   //println(s"max = $max_val")
   println(s"min = $min_val")
   println(s"max = $max_val")
   val filtered_songs_2 = songsDf.where($"year" >= 2000 && $"year" <= 2010)
   val filtered_songs_2_n = filtered_songs_2.count()
   println(s"songs between 2000 inclusive and 2010 inclusive is $filtered_songs_2_n")
  }
}