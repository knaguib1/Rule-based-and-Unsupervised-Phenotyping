package edu.cse6250.main

import edu.cse6250.clustering.Metrics
import edu.cse6250.features.FeatureConstruction
import edu.cse6250.helper.SessionCreator
import edu.cse6250.model.{ Diagnostic, LabResult, Medication }
import edu.cse6250.phenotyping.T2dmPhenotype

import java.text.SimpleDateFormat
import edu.cse6250.helper.{ CSVHelper, SessionCreator }
import edu.cse6250.model.{ Diagnostic, LabResult, Medication }
import org.apache.spark.mllib.clustering.{ GaussianMixture, KMeans, StreamingKMeans }
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.{ DenseMatrix, Matrices, Vector, Vectors }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, SparkSession }

import scala.io.Source

/**
 * @author Hang Su <hangsu@gatech.edu>,
 * @author Yu Jing <yjing43@gatech.edu>,
 * @author Ming Liu <mliu302@gatech.edu>
 */
object Main {
  def main(args: Array[String]) {
    import org.apache.log4j.{ Level, Logger }

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val spark = SessionCreator.spark
    val sc = spark.sparkContext
    //  val sqlContext = spark.sqlContext

    /** initialize loading of data */
    val (medication, labResult, diagnostic) = loadRddRawData(spark)
    val (candidateMedication, candidateLab, candidateDiagnostic) = loadLocalRawData

    /** conduct phenotyping */
    val phenotypeLabel = T2dmPhenotype.transform(medication, labResult, diagnostic)

    /** feature construction with all features */
    val featureTuples = sc.union(
      FeatureConstruction.constructDiagnosticFeatureTuple(diagnostic),
      FeatureConstruction.constructLabFeatureTuple(labResult),
      FeatureConstruction.constructMedicationFeatureTuple(medication))

    // =========== USED FOR AUTO GRADING CLUSTERING GRADING =============
    // phenotypeLabel.map{ case(a,b) => s"$a\t$b" }.saveAsTextFile("data/phenotypeLabel")
    // featureTuples.map{ case((a,b),c) => s"$a\t$b\t$c" }.saveAsTextFile("data/featureTuples")
    // return
    // ==================================================================

    val rawFeatures = FeatureConstruction.construct(sc, featureTuples)

    val (kMeansPurity, gaussianMixturePurity, streamingPurity) = testClustering(phenotypeLabel, rawFeatures)
    println(f"[All feature] getPurity of kMeans is: $kMeansPurity%.5f")
    println(f"[All feature] getPurity of GMM is: $gaussianMixturePurity%.5f")
    println(f"[All feature] getPurity of StreamingKmeans is: $streamingPurity%.5f")

    /** feature construction with filtered features */
    val filteredFeatureTuples = sc.union(
      FeatureConstruction.constructDiagnosticFeatureTuple(diagnostic, candidateDiagnostic),
      FeatureConstruction.constructLabFeatureTuple(labResult, candidateLab),
      FeatureConstruction.constructMedicationFeatureTuple(medication, candidateMedication))

    val filteredRawFeatures = FeatureConstruction.construct(sc, filteredFeatureTuples)

    val (kMeansPurity2, gaussianMixturePurity2, streamingPurity2) = testClustering(phenotypeLabel, filteredRawFeatures)
    println(f"[Filtered feature] getPurity of kMeans is: $kMeansPurity2%.5f")
    println(f"[Filtered feature] getPurity of GMM is: $gaussianMixturePurity2%.5f")
    println(f"[Filtered feature] getPurity of StreamingKmeans is: $streamingPurity2%.5f")
  }

  def testClustering(phenotypeLabel: RDD[(String, Int)], rawFeatures: RDD[(String, Vector)]): (Double, Double, Double) = {
    import org.apache.spark.mllib.linalg.Matrix
    import org.apache.spark.mllib.linalg.distributed.RowMatrix

    println("phenotypeLabel: " + phenotypeLabel.count)
    /** scale features */
    val scaler = new StandardScaler(withMean = true, withStd = true).fit(rawFeatures.map(_._2))
    val features = rawFeatures.map({ case (patientID, featureVector) => (patientID, scaler.transform(Vectors.dense(featureVector.toArray))) })
    println("features: " + features.count)
    val rawFeatureVectors = features.map(_._2).cache()
    println("rawFeatureVectors: " + rawFeatureVectors.count)

    /** reduce dimension */
    val mat: RowMatrix = new RowMatrix(rawFeatureVectors)
    val pc: Matrix = mat.computePrincipalComponents(10) // Principal components are stored in a local dense matrix.
    val featureVectors = mat.multiply(pc).rows

    val densePc = Matrices.dense(pc.numRows, pc.numCols, pc.toArray).asInstanceOf[DenseMatrix]

    def transform(feature: Vector): Vector = {
      val scaled = scaler.transform(Vectors.dense(feature.toArray))
      Vectors.dense(Matrices.dense(1, scaled.size, scaled.toArray).multiply(densePc).toArray)
    }

    /**
     * TODO: K Means Clustering using spark mllib
     * Train a k means model using the variabe featureVectors as input
     * Set maxIterations =20 and seed as 6250L
     * Assign each feature vector to a cluster(predicted Class)
     * Obtain an RDD[(Int, Int)] of the form (cluster number, RealClass)
     * Find Purity using that RDD as an input to Metrics.getPurity
     * Remove the placeholder below after your implementation
     */

    val spark = SessionCreator.spark
    val sc = spark.sparkContext

    val kmeans = new KMeans().setK(3).setSeed(6250L)
    val kmeans_model = kmeans.run(featureVectors)
    val kmeans_model_predictions = kmeans_model.predict(featureVectors).collect()
    val kmeans_clusterAssignmentAndLabel = sc.parallelize(
      kmeans_model_predictions.zip(
        phenotypeLabel.map { row => row._2 }
          .collect)
        .map { row => row })

    val kMeansPurity = Metrics.getPurity(kmeans_clusterAssignmentAndLabel)

    /**
     * TODO: GMMM Clustering using spark mllib
     * Train a Gaussian Mixture model using the variabe featureVectors as input
     * Set maxIterations =20 and seed as 6250L
     * Assign each feature vector to a cluster(predicted Class)
     * Obtain an RDD[(Int, Int)] of the form (cluster number, RealClass)
     * Find Purity using that RDD as an input to Metrics.getPurity
     * Remove the placeholder below after your implementation
     */
    val gmm = new GaussianMixture().setK(3).setSeed(6250L)
    val gmm_model = gmm.run(featureVectors)
    val gmm_model_predictions = gmm_model.predict(featureVectors).collect()
    val gmm_clusterAssignmentAndLabel = sc.parallelize(
      gmm_model_predictions.zip(
        phenotypeLabel.map { row => row._2 }
          .collect)
        .map { row => row })

    val gaussianMixturePurity = Metrics.getPurity(gmm_clusterAssignmentAndLabel)

    /**
     * TODO: StreamingKMeans Clustering using spark mllib
     * Train a StreamingKMeans model using the variabe featureVectors as input
     * Set the number of cluster K = 3, DecayFactor = 1.0, number of dimensions = 10, weight for each center = 0.5, seed as 6250L
     * In order to feed RDD[Vector] please use latestModel, see more info: https://spark.apache.org/docs/2.2.0/api/scala/index.html#org.apache.spark.mllib.clustering.StreamingKMeans
     * To run your model, set time unit as 'points'
     * Assign each feature vector to a cluster(predicted Class)
     * Obtain an RDD[(Int, Int)] of the form (cluster number, RealClass)
     * Find Purity using that RDD as an input to Metrics.getPurity
     * Remove the placeholder below after your implementation
     */
    val km = new StreamingKMeans().setK(3).setDecayFactor(10).setRandomCenters(10, 0.5, 6250L)
    val newmodel = km.latestModel.update(featureVectors, 1.0, "points")
    val km_model_predictions = newmodel.predict(featureVectors).collect()
    val km_clusterAssignmentAndLabel = sc.parallelize(
      km_model_predictions.zip(
        phenotypeLabel.map { row => row._2 }
          .collect)
        .map { row => row })

    val streamKmeansPurity = Metrics.getPurity(km_clusterAssignmentAndLabel)

    (kMeansPurity, gaussianMixturePurity, streamKmeansPurity)
  }

  /**
   * load the sets of string for filtering of medication
   * lab result and diagnostics
   *
   * @return
   */
  def loadLocalRawData: (Set[String], Set[String], Set[String]) = {
    val candidateMedication = Source.fromFile("data/med_filter.txt").getLines().map(_.toLowerCase).toSet[String]
    val candidateLab = Source.fromFile("data/lab_filter.txt").getLines().map(_.toLowerCase).toSet[String]
    val candidateDiagnostic = Source.fromFile("data/icd9_filter.txt").getLines().map(_.toLowerCase).toSet[String]
    (candidateMedication, candidateLab, candidateDiagnostic)
  }

  def sqlDateParser(input: String, pattern: String = "yyyy-MM-dd'T'HH:mm:ssX"): java.sql.Date = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX")
    new java.sql.Date(dateFormat.parse(input).getTime)
  }

  def loadRddRawData(spark: SparkSession): (RDD[Medication], RDD[LabResult], RDD[Diagnostic]) = {
    /* the sql queries in spark required to import sparkSession.implicits._ */
    import spark.implicits._
    import edu.cse6250.helper.CSVHelper
    val sqlContext = spark.sqlContext

    /* a helper function sqlDateParser may useful here */

    /**
     * load data using Spark SQL into three RDDs and return them
     * Hint:
     * You can utilize edu.cse6250.helper.CSVHelper
     * through your sparkSession.
     *
     * This guide may helps: https://bit.ly/2xnrVnA
     *
     * Notes:Refer to model/models.scala for the shape of Medication, LabResult, Diagnostic data type.
     * Be careful when you deal with String and numbers in String type.
     * Ignore lab results with missing (empty or NaN) values when these are read in.
     * For dates, use Date_Resulted for labResults and Order_Date for medication.
     *
     */

    /**
     * TODO: implement your own code here and remove
     * existing placeholder code below
     */
    val encounter: DataFrame = CSVHelper.loadCSVAsTable(
      spark,
      "data\\encounter_INPUT.csv",
      "encounter")

    val encounter_dx: DataFrame = CSVHelper.loadCSVAsTable(
      spark,
      "data\\encounter_dx_INPUT.csv",
      "encounter_dx")

    val med: DataFrame = CSVHelper.loadCSVAsTable(
      spark,
      "data\\medication_orders_INPUT.csv",
      "med")

    val lab: DataFrame = CSVHelper.loadCSVAsTable(
      spark,
      "data\\lab_results_INPUT.csv",
      "lab")

    val medication: RDD[Medication] = sqlContext.sql("select " +
      "string(encounter.Member_ID) as patientID, " +
      "to_date(med.Order_Date, 'yyyy-MM-dd') as Date, " +
      "string(med.Drug_Name) as medicine " +
      "from encounter " +
      "inner join med on encounter.Encounter_ID = med.Encounter_ID ").
      as[Medication].rdd

    val labResult: RDD[LabResult] = sqlContext.sql("select " +
      "string(lab.Member_ID) as patientID, " +
      "to_date(lab.Date_Resulted, 'yyyy-MM-dd') as Date, " +
      "string(lab.Result_Name) as testName, " +
      "double(replace(Numeric_Result, ',', '')) as value " +
      "from lab ").na.drop().
      as[LabResult].rdd

    val diagnostic: RDD[Diagnostic] = sqlContext.sql("select " +
      "string(encounter.Member_ID) as patientID," +
      "string(encounter_dx.Code_ID) as code," +
      "to_date(encounter.Encounter_DateTime, 'yyyy-MM-dd') as Date " +
      "from encounter " +
      "inner join encounter_dx on encounter.Encounter_ID = encounter_dx.Encounter_ID ").
      as[Diagnostic].rdd

    (medication, labResult, diagnostic)
  }

}
