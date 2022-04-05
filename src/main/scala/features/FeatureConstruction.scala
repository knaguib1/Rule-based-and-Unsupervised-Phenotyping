package edu.cse6250.features

import edu.cse6250.helper.SessionCreator
import edu.cse6250.model.{ Diagnostic, LabResult, Medication }
import edu.cse6250.model.{ Diagnostic, LabResult, Medication }
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.rdd.RDD

/**
 * @author Hang Su
 */
object FeatureConstruction {

  /**
   * ((patient-id, feature-name), feature-value)
   */
  type FeatureTuple = ((String, String), Double)

  /**
   * Aggregate feature tuples from diagnostic with COUNT aggregation,
   *
   * @param diagnostic RDD of diagnostic
   * @return RDD of feature tuples
   */
  def constructDiagnosticFeatureTuple(diagnostic: RDD[Diagnostic]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    import org.apache.spark.sql.functions.{ lower, min, col, lit }
    import org.apache.spark.sql.DataFrame
    val spark = SessionCreator.spark
    import spark.implicits._
    val sqlContext = spark.sqlContext

    diagnostic.toDF().groupBy("patientID", "code")
      .count()
      .withColumn("count", col("count").cast("Double"))
      .rdd
      .map(row => {

        val patient = row.getString(0)
        val code = row.getString(1)
        val cnt = row.getDouble(2)

        ((patient, code), cnt)
      })
  }

  /**
   * Aggregate feature tuples from medication with COUNT aggregation,
   *
   * @param medication RDD of medication
   * @return RDD of feature tuples
   */
  def constructMedicationFeatureTuple(medication: RDD[Medication]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    import org.apache.spark.sql.functions.{ lower, min, col, lit }
    import org.apache.spark.sql.DataFrame
    val spark = SessionCreator.spark
    import spark.implicits._
    val sqlContext = spark.sqlContext

    medication.toDF().groupBy("patientID", "medicine")
      .count()
      .withColumn("count", col("count").cast("Double"))
      .rdd
      .map(row => {

        val patient = row.getString(0)
        val med = row.getString(1)
        val cnt = row.getDouble(2)

        ((patient, med), cnt)
      })
  }

  /**
   * Aggregate feature tuples from lab result, using AVERAGE aggregation
   *
   * @param labResult RDD of lab result
   * @return RDD of feature tuples
   */
  def constructLabFeatureTuple(labResult: RDD[LabResult]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    import org.apache.spark.sql.functions.{ lower, min, col, lit }
    import org.apache.spark.sql.DataFrame
    val spark = SessionCreator.spark
    import spark.implicits._
    val sqlContext = spark.sqlContext

    labResult.toDF()
      .groupBy("patientID", "testName")
      .avg("value")
      .withColumn("avg(value)", col("avg(value)").cast("Double"))
      .rdd
      .map(row => {

        val patient = row.getString(0)
        val test = row.getString(1)
        val val_agg = row.getDouble(2)

        ((patient, test), val_agg)
      })

  }

  /**
   * Aggregate feature tuple from diagnostics with COUNT aggregation, but use code that is
   * available in the given set only and drop all others.
   *
   * @param diagnostic   RDD of diagnostics
   * @param candiateCode set of candidate code, filter diagnostics based on this set
   * @return RDD of feature tuples
   */
  def constructDiagnosticFeatureTuple(diagnostic: RDD[Diagnostic], candidateCode: Set[String]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    import org.apache.spark.sql.functions.{ lower, min, col, lit }
    import org.apache.spark.sql.DataFrame
    val spark = SessionCreator.spark
    import spark.implicits._
    val sqlContext = spark.sqlContext

    diagnostic.toDF()
      .filter(col("code").isin(candidateCode.toSeq: _*))
      .groupBy("patientID", "code")
      .count()
      .withColumn("count", col("count").cast("Double"))
      .rdd
      .map(row => {

        val patient = row.getString(0)
        val code = row.getString(1)
        val cnt = row.getDouble(2)

        ((patient, code), cnt)
      })

  }

  /**
   * Aggregate feature tuples from medication with COUNT aggregation, use medications from
   * given set only and drop all others.
   *
   * @param medication          RDD of diagnostics
   * @param candidateMedication set of candidate medication
   * @return RDD of feature tuples
   */
  def constructMedicationFeatureTuple(medication: RDD[Medication], candidateMedication: Set[String]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    import org.apache.spark.sql.functions.{ lower, min, col, lit }
    import org.apache.spark.sql.DataFrame
    val spark = SessionCreator.spark
    import spark.implicits._
    val sqlContext = spark.sqlContext

    medication.toDF()
      .filter(col("medicine").isin(candidateMedication.toSeq: _*))
      .groupBy("patientID", "medicine")
      .count()
      .withColumn("count", col("count").cast("Double"))
      .rdd
      .map(row => {

        val patient = row.getString(0)
        val med = row.getString(1)
        val cnt = row.getDouble(2)

        ((patient, med), cnt)
      })
  }

  /**
   * Aggregate feature tuples from lab result with AVERAGE aggregation, use lab from
   * given set of lab test names only and drop all others.
   *
   * @param labResult    RDD of lab result
   * @param candidateLab set of candidate lab test name
   * @return RDD of feature tuples
   */
  def constructLabFeatureTuple(labResult: RDD[LabResult], candidateLab: Set[String]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    import org.apache.spark.sql.functions.{ lower, min, col, lit }
    import org.apache.spark.sql.DataFrame
    val spark = SessionCreator.spark
    import spark.implicits._
    val sqlContext = spark.sqlContext

    labResult.toDF()
      .filter(col("testName").isin(candidateLab.toSeq: _*))
      .groupBy("patientID", "testName")
      .avg("value")
      .withColumn("avg(value)", col("avg(value)").cast("Double"))
      .rdd
      .map(row => {

        val patient = row.getString(0)
        val test = row.getString(1)
        val val_agg = row.getDouble(2)

        ((patient, test), val_agg)
      })

  }

  /**
   * Given a feature tuples RDD, construct features in vector
   * format for each patient. feature name should be mapped
   * to some index and convert to sparse feature format.
   *
   * @param sc      SparkContext to run
   * @param feature RDD of input feature tuples
   * @return
   */
  def construct(sc: SparkContext, feature: RDD[FeatureTuple]): RDD[(String, Vector)] = {

    import org.apache.spark.mllib.linalg.Vectors
    /** save for later usage */
    feature.cache()

    /** create a feature name to id map */
    val grpPatients = feature.map(row => {

      val patient = row._1._1
      val feature = row._1._2
      val value = row._2

      (patient, (patient, feature, value))

    }).groupByKey()

    val tempFeat = grpPatients.map(row => {

      val patient = row._1
      val featureName = row._2.map(_._1)
      val featureValue = row._2.map(_._2)

      (featureName, featureValue)

    })

    val temp_map = feature.map(row => {

      val feature = row._1._2

      (feature, feature)

    }).distinct.sortBy(_._1).map(r => r._1)

    val featureMap = temp_map.collect.zipWithIndex.toMap

    val scFeatureMap = sc.broadcast(featureMap)
    /** transform input feature */

    /**
     * Functions maybe helpful:
     * collect
     * groupByKey
     */

    val targetFeat = feature.map(row => {

      val patient = row._1._1
      val feature = row._1._2
      val value = row._2

      (patient, (feature, value))

    }).sortBy(_._2)
      .groupByKey()
      .collect
      .sortBy(_._1)

    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    val result = sc.parallelize(
      targetFeat.map {
        case (target, features) =>

          val numFeature = scFeatureMap.value.size
          val indexedFeatures = features.map { row => scFeatureMap.value(row._1) }
          //     val featureVector = Vectors.sparse(numFeature, indexedFeatures)
          //     val labeledPoint = LabeledPoint(target, featureVector)
          val featureValue = features.map { row => row._2 }

          val featureVector = Vectors.sparse(numFeature, indexedFeatures.toArray, featureValue.toArray)

          (target, featureVector)
      })

    result
  }
}

