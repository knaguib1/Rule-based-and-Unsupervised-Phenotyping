/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.cse6250.clustering

import edu.cse6250.helper.SessionCreator
import org.apache.spark.rdd.RDD

object Metrics {
  /**
   * Given input RDD with tuples of assigned cluster id by clustering,
   * and corresponding real class. Calculate the getPurity of clustering.
   * Purity is defined as
   * \fract{1}{N}\sum_K max_j |w_k \cap c_j|
   * where N is the number of samples, K is number of clusters and j
   * is index of class. w_k denotes the set of samples in k-th cluster
   * and c_j denotes set of samples of class j.
   *
   * @param clusterAssignmentAndLabel RDD in the tuple format
   *                                  (assigned_cluster_id, class)
   * @return
   */
  def getPurity(clusterAssignmentAndLabel: RDD[(Int, Int)]): Double = {
    /**
     * TODO: Remove the placeholder and implement your code here
     */
    import org.apache.spark.sql.functions.{ lower, min, col, lit, sum }
    import org.apache.spark.sql.DataFrame
    val spark = SessionCreator.spark
    import spark.implicits._
    val sqlContext = spark.sqlContext

    val clusterAssignments = clusterAssignmentAndLabel.toDF()
      .withColumn("val", lit(1))
      .groupBy(col("_1"), col("_2"))
      .sum("val")

    val maxClusterAssignments = clusterAssignments.groupBy(col("_1"))
      .max("sum(val)")
      .agg(sum("max(sum(val))"))
      .first().getLong(0)

    val clusters = clusterAssignments
      .agg(sum("sum(val)"))
      .first().getLong(0).toDouble

    maxClusterAssignments / clusters
  }
}
