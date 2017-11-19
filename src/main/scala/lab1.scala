package myLab1

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark._
import org.apache.spark.sql.{ SQLContext, DataFrame }
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.RegexTokenizer
import se.kth.spark.lab1._
import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.PolynomialExpansion
import se.kth.spark.lab1.task6.MyLinearRegression
import se.kth.spark.lab1.task6.MyLinearRegressionImpl
import se.kth.spark.lab1.task6.MyLinearModelImpl

case class Song(year: Int, features: Array[Float]) {
  val list_str = features.mkString(",")
  override def toString = s"Song(year = $year,features = [$list_str])"
  //This should be default implementation of toString str???!!!
}

object lab1 {
  val conf = new SparkConf().setAppName("lab1").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._
  import sqlContext._

  val filePath = "src/main/resources/millionsong.txt"
  //Task 1
  val rdd = sc.textFile(filePath)
  val recordsRdd = rdd.map(rec => rec.split(",").map(_.toFloat))
  val songsRdd = recordsRdd.map(rec => Song(rec(0).toInt, rec.slice(1, 4)))
  val songsDf = songsRdd.toDF()
  val n = songsDf.count
  val filteredSongs1 = songsDf.where($"year" >= 1998 && $"year" <= 2000)
  val filteredSongs2 = songsDf.where($"year" >= 2000 && $"year" <= 2010)

  val minMeanMaxYear = songsDf.agg(min($"year"), mean($"year"), max($"year")).first
  val minYear = minMeanMaxYear(0).asInstanceOf[Int].toDouble
  val meanYear = minMeanMaxYear(1).asInstanceOf[Double]
  val maxYear = minMeanMaxYear(2).asInstanceOf[Int].toDouble

  def task1 {
    println(s"dataset rows = $n")
    val filtered_songs_1_n = filteredSongs1.count
    println(s"songs between 1998 inclusive and 2000 inclusive is $filtered_songs_1_n")
    val filtered_songs_2_n = filteredSongs2.count()
    println(s"$filtered_songs_2_n songs between 2000 inclusive and 2010 inclusive")
    println(s"max year  = $maxYear")
    println(s"mean year = $meanYear")
    println(s"min year  = $minYear")
  }

  //Task 2
  val dataColName = "data_vec"
  val vecLabelColName = "label_vec"
  val rawLabelColName = "label_raw"
  val labelColName = "label"
  val featuresColName = "features"
  val featureIdx = 0
  val featureIdxs = Array(1, 2, 3)
  val rawDF = sqlContext.read.text(filePath)
  val regexTokenizer = new RegexTokenizer()
    .setInputCol("value")
    .setOutputCol("data_arr")
    .setPattern(",")
  val arr2Vec = new Array2Vector()
    .setInputCol("data_arr")
    .setOutputCol(dataColName)
  val labelSlicer = new VectorSlicer()
    .setIndices(Array(featureIdx))
    .setInputCol(dataColName)
    .setOutputCol(vecLabelColName)

  val v2d = new Vector2DoubleUDF("year_vec", arr => arr(0).toDouble)
    .setInputCol(vecLabelColName)
    .setOutputCol(rawLabelColName)

  val labelShifter = new DoubleUDF(year => year - minYear)
    .setInputCol(rawLabelColName)
    .setOutputCol(labelColName)

  val featuresSlicer = new VectorSlicer()
    .setIndices(featureIdxs)
    .setInputCol(dataColName)
    .setOutputCol(featuresColName)

  val task2PipelineStages = Array(
    regexTokenizer,
    arr2Vec,
    labelSlicer,
    v2d,
    labelShifter,
    featuresSlicer)

  val task2Pipline = new Pipeline().setStages(task2PipelineStages)

  val task2PiplineModel = task2Pipline.fit(rawDF)

  val dataset = task2PiplineModel.transform(rawDF).select("label", "features")
  dataset.cache()

  def task2 {
    val columns = rawDF.columns.mkString
    println(s"columns = [$columns]")
    println(rawDF.count)

    println("Step2: transform with tokenizer and show 5 rows")
    val meCanParseCSV = regexTokenizer.transform(rawDF)
    meCanParseCSV.show(5)

    println("5 rows of arr2Vec")
    val arr2VecTrans = arr2Vec.transform(meCanParseCSV)
    arr2VecTrans.select(dataColName).take(5).foreach(row => println(row.mkString(",")))

    println("5 rows of vector labels")
    val lSlicerTrans = labelSlicer.transform(arr2VecTrans)
    lSlicerTrans.select("label_vec").show(5)

    println("5 rwos of labels")
    val v2dTrans = v2d.transform(lSlicerTrans)
    v2dTrans.select(rawLabelColName).show(5)

    println("5 rows of shifted labels")
    val lShifterTrans = labelShifter.transform(v2dTrans)
    lShifterTrans.select(labelColName).show(5)

    println("5 rows of features")
    featuresSlicer.transform(v2dTrans).select("features").show(5)

    println("5 rows of dataset using pipeline")
    dataset.show(5)
  }

  //Task 3
  val predictionColName = "prediction"
  val trainTestSplit = Array(0.8, 0.2)
  val Array(trainRaw, testRaw) = rawDF.randomSplit(trainTestSplit, seed = 1)

  def getRegressor(labelCol: String,featuresCol : String): LinearRegression = {
    new LinearRegression()
      .setElasticNetParam(0.1)
      .setRegParam(0.1)
      .setMaxIter(10)
      .setLabelCol(labelCol)
      .setFeaturesCol(featuresCol)
      .setPredictionCol(predictionColName)
  }
  val regressor = getRegressor(labelColName, featuresColName)

  val task3PipelineStages = task2PipelineStages :+ regressor
  val regressorIdx = task3PipelineStages.length - 1
  val task3Pipeline = new Pipeline().setStages(task3PipelineStages)

  def task3 {
    val task3Model: PipelineModel = task3Pipeline.fit(trainRaw)
    val lrModel = task3Model.stages(regressorIdx).asInstanceOf[LinearRegressionModel]
    val rmse = lrModel.summary.rootMeanSquaredError
    println(s"rmse = $rmse") //rmse = 17.307773680962672
    println("5 rows of predictions on test set")
    task3Model.transform(testRaw).select(predictionColName).show(5)
  }

  //Task 4
  val paramGrid = new ParamGridBuilder()
    .addGrid(regressor.maxIter, Array(6, 8, 10, 12, 14))
    .addGrid(regressor.regParam, Array(0.02, 0.08, 0.1, 1.2, 1.4))
    .build()

  def getCrossValidator(estimator: Pipeline): CrossValidator = {
    new CrossValidator()
      .setEstimator(estimator)
      .setEvaluator(new RegressionEvaluator())
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(10)
  }
  val cvTask4 = getCrossValidator(task3Pipeline)
  def evaluateCvModel(cv: CrossValidator,regIdx : Int) {
    println("starting training")
    val cvModel: CrossValidatorModel = cv.fit(trainRaw)
    val lrModel = cvModel.bestModel.asInstanceOf[PipelineModel].stages(regIdx).asInstanceOf[LinearRegressionModel]
    val rmse = lrModel.summary.rootMeanSquaredError
    println(s"rmse = $rmse")
  }

  def task4 {
    evaluateCvModel(cvTask4,regressorIdx)
    // rmse = 17.3077399756819
  }

  //Task 5
  val polyFeaturesColName = "poly_features"
  val polyExp = new PolynomialExpansion()
    .setInputCol(featuresColName)
    .setOutputCol(polyFeaturesColName)
    .setDegree(2)
  val polyRegressor = getRegressor(labelColName,polyFeaturesColName)
  val task5PipelineStages = task2PipelineStages :+ polyExp :+ polyRegressor
  val task5Pipeline = new Pipeline().setStages(task5PipelineStages)
  val cvTask5 = getCrossValidator(task5Pipeline)
  val task5RegressorIdx = task5PipelineStages.length - 1
  def task5 {
    evaluateCvModel(cvTask5,task5RegressorIdx)
    //rmse = 17.270099794875186
  }
  
  //Task 6
  val myRegressor = new MyLinearRegressionImpl()
    .setFeaturesCol(featuresColName)
    .setLabelCol(labelColName)
    .setPredictionCol(predictionColName)

  val task6PiplineStages = task2PipelineStages :+ myRegressor

  val task6Pipeline = new Pipeline().setStages(task6PiplineStages)

  def task6 {
    val task6Model: PipelineModel = task6Pipeline.fit(trainRaw)
    val lrModel = task6Model.stages(regressorIdx).asInstanceOf[MyLinearModelImpl]
    println("training errors = ",lrModel.trainingError.mkString(","))
    //    println(s"rmse = $rmse") //rmse = 17.307773680962672
    println("5 rows of predictions on test set")
    val res = task6Model.transform(testRaw).select(predictionColName).show()
  }
}