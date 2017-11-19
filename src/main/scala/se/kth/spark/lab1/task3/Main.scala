package se.kth.spark.lab1.task3
//import se.kth.spark.lab1._
//import org.apache.spark._
//import org.apache.spark.sql.{ SQLContext, DataFrame }
//import org.apache.spark.ml.tuning.CrossValidatorModel
//import org.apache.spark.ml.regression.LinearRegressionModel
//import org.apache.spark.ml.PipelineModel
//
//import org.apache.spark.ml.feature.RegexTokenizer
//import org.apache.spark.ml.Pipeline
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.apache.spark.sql.SQLContext
//import org.apache.spark.ml.feature.VectorSlicer
//import org.apache.spark.ml.regression.LinearRegression
import myLab1.lab1
object Main {
  def main(args: Array[String]) {
    lab1.task3
    
//    val conf = new SparkConf().setAppName("lab1").setMaster("local")
//    val sc = new SparkContext(conf)
//    val sqlContext = new SQLContext(sc)
//    import org.apache.spark.sql.functions._
//    import sqlContext.implicits._
//    import sqlContext._
//
//    val filePath = "src/main/resources/millionsong.txt"
//    val rawDF = sqlContext.read.text(filePath)
//
//    //Step1: tokenize each row
//    val regexTokenizer = new RegexTokenizer()
//      .setInputCol("value")
//      .setOutputCol("feat_arr")
//      .setPattern(",")
//
//    //Step2: transform with tokenizer and show 5 rows
//    val csvParse = regexTokenizer.transform(rawDF)
//
//    //Step3: transform array of tokens to a vector of tokens (use our ArrayToVector)
//    val arr2Vect = new Array2Vector()
//      .setInputCol("feat_arr")
//      .setOutputCol("feat_vec")
//    val arr2VectTrans = arr2Vect.transform(csvParse)
//
//    //Step4: extract the label(year_vec) into a new column
//    val lSlicer = new VectorSlicer()
//      .setIndices(Array(0))
//      .setInputCol("feat_vec")
//      .setOutputCol("year_vec")
//
//    val lSlicerTrans = lSlicer.transform(arr2VectTrans)
//    
//    //Step5: convert type of the label from vector to double (use our Vector2Double)
//    val v2d = new Vector2DoubleUDF("year_vec", arr => arr(0).toDouble)
//      .setInputCol("year_vec")
//      .setOutputCol("year")
//
//    val v2dTrans = v2d.transform(lSlicerTrans)
//
//    //Step6: shift all labels by the value of minimum label such that the value of the smallest becomes 0 (use our DoubleUDF)
//    val min_year = v2dTrans.select("year").agg(min($"year")).first()(0).asInstanceOf[Double]
//    val lShifter = new DoubleUDF(year => year - min_year)
//      .setInputCol("year")
//      .setOutputCol("label")
//    val lShifterTrans = lShifter.transform(v2dTrans)
//
//    //Step7: extract just the 3 first features in a new vector column
//    val fSlicer = new VectorSlicer()
//      .setIndices(Array(1, 2, 3))
//      .setInputCol("feat_vec")
//      .setOutputCol("features")
//    
//    val regressor = new LinearRegression()
//      .setElasticNetParam(0.1)
//      .setRegParam(0.1)
//      .setMaxIter(10)
//      .setLabelCol("label")
//      .setFeaturesCol("features")
//      .setPredictionCol("pred")
//
//    val pipeStages = Array(
//        regexTokenizer,
//        arr2Vect,
//        lSlicer,
//        v2d,
//        lShifter,
//        fSlicer,
//        regressor
//      )
//    val pipeline = new Pipeline().setStages(pipeStages)
//    val lrStage = pipeStages.length - 1
//    
//    val pipelineModel: PipelineModel = pipeline.fit(rawDF)
//    val lrModel = pipelineModel.stages(lrStage).asInstanceOf[LinearRegressionModel]
//		val rmse = lrModel.summary.rootMeanSquaredError
//		println(s"rmse = $rmse")
//    println("done")
//    val test_set = ???
//    val res = lrModel.transform(test_set)
//    res.select("pred").show()
  }
}