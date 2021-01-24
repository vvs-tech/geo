import scala.io.Source
import reflect.io._
import Path._
import java.io.File
import java.nio.file._
import java.util.Calendar
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SQLContext, hive}
import org.apache.parquet.io.api.Binary
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object Mail_Loop {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]") //.set('spark.rapids.sql.enabled','true')
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .setAppName("Mail_Transf_sp3")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext: SQLContext = new SQLContext(sc)

    //val pathParquetSrc = "C:\\Programming\\vtb\\process_mailru\\geo\\" //Директория таблицы
    //val pathTemp = "C:\\Programming\\vtb\\process_mailru\\geo_tmp_part\\tmp_"
    val pathParquetSrc = "R:\\geo\\" //Директория таблицы
    val pathTemp = "R:\\geo_tmp\\tmp_"

    val path_p1 = pathParquetSrc + "mailru_portrait\\datep=2020-07-31"
    val path_p2 = pathParquetSrc + "mailru_portrait\\datep=2020-08-31"
    val path_p3 = pathParquetSrc + "mailru_portrait\\datep=2020-09-30"
    //val path_net = "C:\\Programming\\vtb\\process_mailru\\geo\\rf_net_500x500_with_city_id\\"
    //val nameDirTable = nameTableDirectories(pathParquetSrc) //Отдельно название директории таблицы
    //    val listDirAll = new ArrayBuffer[String]                                                     //Массив для дерева каталогов
    //val listDF = new ArrayBuffer[DataFrame] //Массив исходных DF
    val mode = "train"

    val listPartition = new ArrayBuffer[String]                                                          //Массив для партиций


    //listDirAll = dirArray(pathParquetSrc, listDirAll)
    import sqlContext.implicits._

    // ------------------prepare_base_dataframes--------------------------------------------------------------------------
    val net_df = sqlContext.read.parquet(pathParquetSrc + "rf_net_500x500_with_city_id")
      .withColumn("mzone_id", col("zone_id") % 24).repartition($"mzone_id") //.sort($"mzone_id", $"zone_id")
      //.coalesce(24)

    val df_nz = sqlContext.read.parquet(pathParquetSrc + "m")
    //.withColumn("mzone_id", col("zone_id") % 8)
    //.withColumn("mzone_id", col("near_zone_id") % 8).repartition($"mzone_id")

    val base_df = sqlContext.read.parquet(path_p1).select("zone_id", "interest_id", "part_value")
      .unionAll(sqlContext.read.parquet(path_p2).select("zone_id", "interest_id", "part_value"))
      .unionAll(sqlContext.read.parquet(path_p3).select("zone_id", "interest_id", "part_value"))
      .withColumn("mzone_id", col("zone_id") % 24).repartition($"mzone_id") //.sort($"mzone_id", $"zone_id")
      //.coalesce(24)

    val int_x_fdl = sqlContext.read.parquet(pathParquetSrc + "int_x_fld")

    //println("======base_df========= " + base_df.rdd.partitions.size + "++++++++++++++++++++++++++++++++++++++=====================================")
    //println("======net_df ========= " + net_df.rdd.partitions.size + "++++++++++++++++++++++++++++++++++++++=====================================")


    // ------------------prepare_base_dataframes--------------------------------------------------------------------------

    //    base_df.coalesce(8).write.mode("overwrite").partitionBy("mzone_id").parquet("C:\\Programming\\vtb\\process_mailru\\geo_tmp\\mailru_portrait")
    //    net_df.coalesce(8).write.mode("overwrite").partitionBy("mzone_id").parquet("C:\\Programming\\vtb\\process_mailru\\geo_tmp\\rf_net_500x500_with_city_id")
    //
    //    val base_df_p = sqlContext.read.parquet("C:\\Programming\\vtb\\process_mailru\\geo_tmp\\mailru_portrait")
    //    val net_df_p = sqlContext.read.parquet("C:\\Programming\\vtb\\process_mailru\\geo_tmp\\rf_net_500x500_with_city_id")


    //df_nz.show(20, false)
    //println("-------------df_nz COUNT DISTINCT" + df_nz.dropDuplicates("near_zone_id").count())
    //println("-------------df_nz COUNT DISTINCT    " + df_nz.distinct().count())

    net_df.repartition($"mzone_id").write.partitionBy("mzone_id").mode("overwrite").parquet(pathTemp + mode + "_net_df")
    base_df.repartition($"mzone_id").write.partitionBy("mzone_id").mode("overwrite").parquet(pathTemp + mode + "_base_df")

    val net_df_part = sqlContext.read.parquet(pathTemp + mode + "_net_df").persist(StorageLevel.MEMORY_ONLY)
    val base_df_part = sqlContext.read.parquet(pathTemp + mode + "_base_df").persist(StorageLevel.MEMORY_ONLY)

    println("======base_df_part========= " + base_df_part.rdd.partitions.size + "++++++++++++++++++++++++++++++++++++++=====================================")
    println("======net_df_part ========= " + net_df_part.rdd.partitions.size + "++++++++++++++++++++++++++++++++++++++=====================================")

    // ------------------prepare_base_aggregates--------------------------------------------------------------------------
    println(" Processing mailru base aggregates phase started...")
    println(Calendar.getInstance().getTime())
    val start_t1 = Calendar.getInstance().getTime()


    val data_frame = base_df_part.join(net_df_part, base_df_part("zone_id") === net_df_part("zone_id")
      && base_df_part("mzone_id") === net_df_part("mzone_id")
      , "left")
      //.select(base_df("zone_id"), net_df("city_id"), base_df("interest_id"), base_df("part_value"))
      .select(base_df_part("zone_id"), net_df_part("city_id"), base_df_part("interest_id"), base_df_part("part_value"), base_df_part("mzone_id"))
      //.coalesce(24)
      .repartition($"mzone_id")
      .persist(StorageLevel.MEMORY_ONLY)

    println("=====data_frame========== " + data_frame.rdd.partitions.size + "++++++++++++++++++++++++++++++++++++++=====================================")
/*
    val primary_zone = data_frame.groupBy($"zone_id", $"city_id", $"interest_id").agg(
      //avg($"part_value").alias("avg_value_src"),
      round(avg($"part_value"), 3).cast("float").alias("avg_value"),
      max($"mzone_id").alias("primary_zone_part")
    ) //.withColumn("mzone_id", col("zone_id") % 8)
      .coalesce(24)

    primary_zone.show(30, false)
    //    primary_zone.printSchema()
    //    println("-------------primary_zone COUNT " + primary_zone.count())

    println("=============== " + primary_zone.rdd.partitions.size + "++++++++++++++++++++++++++++++++++++++=====================================")
    //primary_zone.coalesce(24).write.mode("overwrite").parquet(pathTemp + mode + "_mailru_primary_zone_city")
    primary_zone.write.mode("overwrite").parquet(pathTemp + mode + "_mailru_primary_zone_city")
*/


  data_frame.write.mode("overwrite").parquet(pathTemp + mode + "_mailru_data_frame")

    //data_frame.show(20,false)
    //println("-------------data_frame COUNT " + data_frame.count())


  //data_frame.groupBy($"mzone_id")

//  data_frame.groupBy($"mzone_id").agg(
//    count("mzone_id").alias("cnt_mzone_id")
//  ).sort($"mzone_id")
//   .show(30, false)




  println("  Processing mailru base aggregates phase finished....")
  println("Start: " + start_t1 + "      End: " + Calendar.getInstance().getTime())

  // ------------------prepare_base_aggregates--------------------------------------------------------------------------



  }

  //def


}