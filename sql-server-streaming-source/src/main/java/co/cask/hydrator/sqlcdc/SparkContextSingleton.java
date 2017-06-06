package co.cask.hydrator.sqlcdc;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

/**
 * Created by rsinha on 6/6/17.
 */
class SparkContextSingleton {
  private static transient SparkContext instance = null;

  public static SparkContext getInstance(SparkConf sparkConf) {
    if (instance == null) {
      instance = new SparkContext(sparkConf);
    }
    return instance;
  }
}
