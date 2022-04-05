package edu.cse6250.phenotyping

import edu.cse6250.model.{ Diagnostic, LabResult, Medication }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import edu.cse6250.helper.{ CSVHelper, SessionCreator }
import org.apache.spark.sql.functions.{ lower, min, col, lit }

/**
 * @author Hang Su <hangsu@gatech.edu>,
 * @author Sungtae An <stan84@gatech.edu>,
 */
object T2dmPhenotype {

  /** Hard code the criteria */
  val T1DM_DX = Set("250.01", "250.03", "250.11", "250.13", "250.21", "250.23", "250.31", "250.33", "250.41", "250.43",
    "250.51", "250.53", "250.61", "250.63", "250.71", "250.73", "250.81", "250.83", "250.91", "250.93")

  val T2DM_DX = Set("250.3", "250.32", "250.2", "250.22", "250.9", "250.92", "250.8", "250.82", "250.7", "250.72", "250.6",
    "250.62", "250.5", "250.52", "250.4", "250.42", "250.00", "250.02")

  val T1DM_MED = Set("lantus", "insulin glargine", "insulin aspart", "insulin detemir", "insulin lente", "insulin nph", "insulin reg", "insulin,ultralente")

  val T2DM_MED = Set("chlorpropamide", "diabinese", "diabanase", "diabinase", "glipizide", "glucotrol", "glucotrol xl",
    "glucatrol ", "glyburide", "micronase", "glynase", "diabetamide", "diabeta", "glimepiride", "amaryl",
    "repaglinide", "prandin", "nateglinide", "metformin", "rosiglitazone", "pioglitazone", "acarbose",
    "miglitol", "sitagliptin", "exenatide", "tolazamide", "acetohexamide", "troglitazone", "tolbutamide",
    "avandia", "actos", "actos", "glipizide")

  /**
   * Transform given data set to a RDD of patients and corresponding phenotype
   *
   * @param medication medication RDD
   * @param labResult  lab result RDD
   * @param diagnostic diagnostic code RDD
   * @return tuple in the format of (patient-ID, label). label = 1 if the patient is case, label = 2 if control, 3 otherwise
   */
  def transform(medication: RDD[Medication], labResult: RDD[LabResult], diagnostic: RDD[Diagnostic]): RDD[(String, Int)] = {
    /**
     * Remove the place holder and implement your code here.
     * Hard code the medication, lab, icd code etc. for phenotypes like example code below.
     * When testing your code, we expect your function to have no side effect,
     * i.e. do NOT read from file or write file
     *
     * You don't need to follow the example placeholder code below exactly, but do have the same return type.
     *
     * Hint: Consider case sensitivity when doing string comparisons.
     */

    val spark = SessionCreator.spark
    import spark.implicits._
    val sqlContext = spark.sqlContext

    val sc = spark.sparkContext
    /** Hard code the criteria */
    // val type1_dm_dx = Set("code1", "250.03")
    // val type1_dm_med = Set("med1", "insulin nph")
    // use the given criteria above like T1DM_DX, T2DM_DX, T1DM_MED, T2DM_MED and hard code DM_RELATED_DX criteria as well

    val DM_RELATED_DX = Set("790.21", "790.22", "790.2", "790.29", "648.81", "648.82", "648.83",
      "648.84", "648", "648", "648.01", "648.02", "648.03",
      "648.04", "791.5", "277.7", "V77.1", "256.4", "250.*")

    import org.apache.spark.sql.functions.{ lower, min, col, lit }

    val dx_df = diagnostic.toDF()
      .filter((!col("code").isin(T1DM_DX.toSeq: _*)) && (col("code").isin(T2DM_DX.toSeq: _*)))
      .select("patientID")
      .distinct()

    val dx_case_patients = dx_df.rdd.map(r => r(0)).collect()

    val med_df = medication.toDF()

    val med_sub1 = med_df.filter((col("patientID").isin(dx_case_patients.toSeq: _*))
      && (lower(col("medicine")).isin(T1DM_MED.toSeq: _*)))

    val case1 = dx_df.join(med_sub1, dx_df("patientID") === med_sub1("patientID"), "leftanti")
      .select("patientID")
      .distinct()

    // turn unique columns to list
    val med_pat1 = med_sub1.select("patientID")
      .distinct()
      .rdd.map(r => r(0)).collect()

    val med_sub2 = med_df.filter((col("patientID").isin(med_pat1.toSeq: _*))
      && (lower(col("medicine")).isin(T2DM_MED.toSeq: _*)))

    val med_pat2 = med_sub2.select("patientID")
      .distinct()
      .rdd.map(r => r(0)).collect()

    val case2 = med_df.filter((col("patientID").isin(med_pat1.toSeq: _*))
      && (!col("patientID").isin(med_pat2.toSeq: _*))).select("patientID").distinct()

    val t1_df = med_df.filter((col("patientID").isin(med_pat2.toSeq: _*))
      && (lower(col("medicine")).isin(T1DM_MED.toSeq: _*)))
      .groupBy(col("patientID")).agg(min(col("Date")))
      .withColumnRenamed("min(Date)", "t1")
      .withColumnRenamed("patientID", "t1_patient")

    val t2_df = med_df.filter((col("patientID").isin(med_pat2.toSeq: _*))
      && (lower(col("medicine")).isin(T2DM_MED.toSeq: _*)))
      .groupBy(col("patientID")).agg(min(col("Date")))
      .withColumnRenamed("min(Date)", "t2")

    val case3 = t2_df.join(t1_df, t2_df("patientID") === t1_df("t1_patient"))
      .filter(col("t2") < col("t1"))
      .select(col("patientID"))
      .distinct()

    val all_case_patients = case1.union(case2).union(case3)

    val casePatients = all_case_patients
      .withColumn("label", lit(1))
      .rdd
      .map(row => {

        val id = row.getString(0)
        val label = row.getInt(1)

        (id, label)

      })

    val lab_df = labResult.toDF().withColumn("testName", lower(col("testName")))

    val lab_pat1 = lab_df.filter(col("testName").contains("glucose"))
      .select(col("patientID"))
      .distinct()

    val lab_temp1 = lab_pat1.rdd.map(r => r(0)).collect()

    val lab_pat2 = lab_df.filter((col("patientID").isin(lab_temp1.toSeq: _*)) &&
      (
        ((col("testName") === "hba1c") && (col("value") >= 6))
        or ((col("testName") === "hemoglobin a1c") && (col("value") >= 6))
        or ((col("testName") === "fasting glucose") && (col("value") >= 110))
        or ((col("testName") === "fasting blood glucose") && (col("value") >= 110))
        or ((col("testName") === "fasting plasma glucose") && (col("value") >= 110))
        or ((col("testName") === "glucose") && (col("value") > 110))
        or ((col("testName") === "glucose, serum") && (col("value") > 110))))
      .select(col("patientID"))
      .distinct()

    val lab_temp2 = lab_pat2.rdd.map(r => r(0)).collect()

    val lab_pat3 = lab_df.filter((col("patientID").isin(lab_temp1.toSeq: _*)) &&
      (!col("patientID").isin(lab_temp2.toSeq: _*)))
      .select(col("patientID"))
      .distinct()

    val lab_temp3 = lab_pat3.rdd.map(r => r(0)).collect()

    val lab_pat4 = diagnostic.toDF().filter((col("code").isin(DM_RELATED_DX.toSeq: _*))
      or (col("code").contains("250.")))

    val lab_temp4 = lab_pat4.rdd.map(r => r(0)).collect()

    val control_patients = lab_df.filter((col("patientID").isin(lab_temp3.toSeq: _*)) &&
      (!col("patientID").isin(lab_temp4.toSeq: _*))).select(col("patientID"))
      .distinct()

    val controlPatients = control_patients.withColumn("label", lit(2))
      .rdd
      .map(row => {

        val id = row.getString(0)
        val label = row.getInt(1)

        (id, label)
      })

    val all_patients = all_case_patients.union(control_patients).rdd.map(r => r(0)).collect()

    val others = diagnostic.toDF().filter(!col("patientID").isin(all_patients.toSeq: _*))
      .select(col("patientID"))
      .distinct()
      .withColumn("label", lit(3))
      .rdd
      .map(row => {

        val id = row.getString(0)
        val label = row.getInt(1)

        (id, label)
      })

    /** Once you find patients for each group, make them as a single RDD[(String, Int)] */
    val phenotypeLabel = sc.union(casePatients, controlPatients, others)

    /** Return */
    phenotypeLabel
  }

  /**
   * calculate specific stats given phenotype labels and corresponding data set rdd
   * @param labResult  lab result RDD
   * @param phenotypeLabel phenotype label return from T2dmPhenotype.transfrom
   * @return tuple in the format of (case_mean, control_mean, other_mean).
   *         case_mean = mean Glucose lab test results of case group
   *         control_mean = mean Glucose lab test results of control group
   *         other_mean = mean Glucose lab test results of unknown group
   *         Attention: order of the three stats in the returned tuple matters!
   */
  def stat_calc(labResult: RDD[LabResult], phenotypeLabel: RDD[(String, Int)]): (Double, Double, Double) = {
    /**
     * you need to hardcode the feature name and the type of stat:
     * e.g. calculate "mean" of "Glucose" lab test result of each group: case, control, unknown
     *
     * The feature name should be "Glucose" exactly with considering case sensitivity.
     * i.e. "Glucose" and "glucose" are counted, but features like "fasting glucose" should not be counted.
     *
     * Hint: rdd dataset can directly call statistic method. Details can be found on course website.
     *
     */

    val spark = SessionCreator.spark
    import spark.implicits._
    val sqlContext = spark.sqlContext

    val sc = spark.sparkContext

    val patient_df = phenotypeLabel.toDF()
    val col1 = patient_df.columns(0)
    val col2 = patient_df.columns(1)

    val lab_df = labResult.toDF()

    val lab_stats = lab_df.join(patient_df, lab_df("patientID") === patient_df(col1))
      .withColumn("value", col("value").cast("double"))
      .withColumn("testName", lower(col("testName")))
      .filter(col("testName") === "glucose")
      .groupBy(col(col2))
      .avg("value")

    lab_stats.show()

    val case_mean = lab_stats.filter(col(col2) === 1).select(col("avg(value)")).head().getDouble(0)
    val control_mean = lab_stats.filter(col(col2) === 2).select(col("avg(value)")).head().getDouble(0)
    val other_mean = lab_stats.filter(col(col2) === 3).select(col("avg(value)")).head().getDouble(0)

    (case_mean, control_mean, other_mean)
  }
}