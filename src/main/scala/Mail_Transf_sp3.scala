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

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer


object Mail_Transf_sp3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]")//.set('spark.rapids.sql.enabled','true')
      .set("spark.dynamicAllocation.enabled", "true")
      .setAppName("Mail_Transf_sp3")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext: SQLContext = new SQLContext(sc)

    val pathParquetSrc = "C:\\Programming\\vtb\\process_mailru\\geo\\" //Директория таблицы

    val path_p1 = pathParquetSrc + "mailru_portrait\\datep=2020-07-31"
    val path_p2 = pathParquetSrc + "mailru_portrait\\datep=2020-08-31"
    val path_p3 = pathParquetSrc + "mailru_portrait\\datep=2020-09-30"
    //val path_net = "C:\\Programming\\vtb\\process_mailru\\geo\\rf_net_500x500_with_city_id\\"
    //val nameDirTable = nameTableDirectories(pathParquetSrc) //Отдельно название директории таблицы
    //    val listDirAll = new ArrayBuffer[String]                                                     //Массив для дерева каталогов
    //val listDF = new ArrayBuffer[DataFrame] //Массив исходных DF
    val mode = "train"


    //listDirAll = dirArray(pathParquetSrc, listDirAll)
    import sqlContext.implicits._

    // ------------------prepare_base_dataframes--------------------------------------------------------------------------
    val net_df = sqlContext.read.parquet(pathParquetSrc + "rf_net_500x500_with_city_id")
      //.withColumn("mzone_id", col("zone_id") % 8).repartition($"mzone_id") //.sort($"mzone_id", $"zone_id")

    val df_nz = sqlContext.read.parquet(pathParquetSrc + "m")
      //.withColumn("mzone_id", col("zone_id") % 8)
      //.withColumn("mzone_id", col("near_zone_id") % 8).repartition($"mzone_id")

    val base_df = sqlContext.read.parquet(path_p1).select("zone_id", "interest_id", "part_value")
      .unionAll(sqlContext.read.parquet(path_p2).select("zone_id","interest_id", "part_value"))
      .unionAll(sqlContext.read.parquet(path_p3).select("zone_id","interest_id", "part_value"))
      //.withColumn("mzone_id", col("zone_id") % 8).repartition($"mzone_id") //.sort($"mzone_id", $"zone_id")

    val int_x_fdl = sqlContext.read.parquet(pathParquetSrc + "int_x_fld")

    // ------------------prepare_base_dataframes--------------------------------------------------------------------------

    //    base_df.coalesce(8).write.mode("overwrite").partitionBy("mzone_id").parquet("C:\\Programming\\vtb\\process_mailru\\geo_tmp\\mailru_portrait")
    //    net_df.coalesce(8).write.mode("overwrite").partitionBy("mzone_id").parquet("C:\\Programming\\vtb\\process_mailru\\geo_tmp\\rf_net_500x500_with_city_id")
    //
    //    val base_df_p = sqlContext.read.parquet("C:\\Programming\\vtb\\process_mailru\\geo_tmp\\mailru_portrait")
    //    val net_df_p = sqlContext.read.parquet("C:\\Programming\\vtb\\process_mailru\\geo_tmp\\rf_net_500x500_with_city_id")


    //df_nz.show(20, false)
    //println("-------------df_nz COUNT DISTINCT" + df_nz.dropDuplicates("near_zone_id").count())
    //println("-------------df_nz COUNT DISTINCT    " + df_nz.distinct().count())


/*
    // ------------------prepare_base_aggregates--------------------------------------------------------------------------
    println(" Processing mailru base aggregates phase started...")
    println(Calendar.getInstance().getTime())
    val start_t1 = Calendar.getInstance().getTime()


        val data_frame = base_df.join(net_df, base_df("zone_id") === net_df("zone_id")
          //&& base_df("mzone_id") === net_df("mzone_id")
          , "left")
          .select(base_df("zone_id"), net_df("city_id"), base_df("interest_id"), base_df("part_value"))
          //.select(base_df("zone_id"), net_df("city_id"), base_df("interest_id"), base_df("part_value"), base_df("mzone_id"))

        val primary_zone = data_frame.groupBy($"zone_id", $"city_id", $"interest_id").agg(
          //avg($"part_value").alias("avg_value_src"),
          round(avg($"part_value"), 3).cast("float").alias("avg_value"),
        ) //.withColumn("mzone_id", col("zone_id") % 8)


//    primary_zone.show(30, false)
//    primary_zone.printSchema()
//    println("-------------primary_zone COUNT " + primary_zone.count())


    primary_zone.coalesce(48).write.mode("overwrite").parquet(
          "C:\\Programming\\vtb\\process_mailru\\geo_tmp\\tmp_" + mode + "_mailru_primary_zone_city")

    println("  Processing mailru base aggregates phase finished....")
    println("Start: " + start_t1 + "      End: " + Calendar.getInstance().getTime())

    // ------------------prepare_base_aggregates--------------------------------------------------------------------------






    // ------------------prepare_city_aggregates--------------------------------------------------------------------------

    println(" Processing mailru city aggregates phase started...")
    println(Calendar.getInstance().getTime())
    val start_t2 = Calendar.getInstance().getTime()

    val city_aggregates_df = sqlContext.read.parquet(
        "C:\\Programming\\vtb\\process_mailru\\geo_tmp\\tmp_" + mode + "_mailru_primary_zone_city").groupBy($"city_id", $"interest_id").agg(
       round(avg(when($"city_id" =!= -1, $"avg_value").otherwise(null)), 3).cast("float").alias("avg_value_city_relative")
    )

//    city_aggregates_df.show(20, false)
//    city_aggregates_df.printSchema()
//    city_aggregates_df.explain()
//    println("--------- city_aggregates_df  COUNT: " + city_aggregates_df.count())

    city_aggregates_df.coalesce(48).write.mode("overwrite").parquet("C:\\Programming\\vtb\\process_mailru\\geo_tmp\\tmp_" + mode + "_mailru_agg_city")

    println("  Processing mailru city aggregates phase finished...." + "\n")
    println("Start: " + start_t2 + "      End: " + Calendar.getInstance().getTime() + "\n")

    // ------------------prepare_city_aggregates--------------------------------------------------------------------------




    // ------------------prepare_neigbour_aggregates--------------------------------------------------------------------------
    println(" Processing prepare_neigbour_aggregates phase started...")
    println(Calendar.getInstance().getTime())
    val start_t3 = Calendar.getInstance().getTime()

    val primary_aggregates = sqlContext.read.parquet(
      "C:\\Programming\\vtb\\process_mailru\\geo_tmp\\tmp_" + mode + "_mailru_primary_zone_city")
      .select("zone_id","interest_id", "avg_value")
      //.withColumn("pa_mzone_id", col("zone_id") % 8)
      .withColumnRenamed("zone_id", "pa_zone_id") //.repartition($"pa_mzone_id")

    val near_df = df_nz.join(
      primary_aggregates, df_nz("near_zone_id") === primary_aggregates("pa_zone_id"), "inner")
      //primary_aggregates, df_nz("near_zone_id") === primary_aggregates("pa_zone_id") && df_nz("mzone_id") === primary_aggregates("pa_mzone_id"), "inner")
      //primary_aggregates, df_nz("near_zone_id") === primary_aggregates("zone_id"), "inner")

//    primary_aggregates.show(30, false)
//    println("-------------primary_aggregates---------------------")
//
//    near_df.show(30, false)
//    println("-------------near_df---------------------COUNT " + near_df.count())
    println("-------------near_df---------------------")


    //near_df.explain("formatted")
    //near_df.explain()
//    near_df.drop("mzone_id", "near_zone_id", "pa_zone_id", "pa_mzone_id")
//      .coalesce(8).write.mode("overwrite").parquet("C:\\Programming\\vtb\\process_mailru\\geo_tmp\\tmp_" + mode + "_mailru_neighbour_raw")

    near_df.drop("near_zone_id", "pa_zone_id", "pa_mzone_id")
      .coalesce(48).write.mode("overwrite").parquet("C:\\Programming\\vtb\\process_mailru\\geo_tmp\\tmp_" + mode + "_mailru_neighbour_raw")


    val df_zone_id = sqlContext.read.parquet(
      "C:\\Programming\\vtb\\process_mailru\\geo_tmp\\tmp_" + mode + "_mailru_neighbour_raw")
      .groupBy(col("zone_id"), col("interest_id")).agg(
      max(col("avg_value")).alias("avg_value_neighbour_max"),
      round(avg(col("avg_value")), 3).cast("float").alias("avg_value_neighbour_mean"))

    /*
        val df_zone_id_tmp = sqlContext.read.parquet(
          "C:\\Programming\\vtb\\process_mailru\\geo_tmp\\tmp_" + mode + "_mailru_neighbour_raw")

        df_zone_id_tmp.createOrReplaceTempView("df_zone_id_v")

        val df_zone_id = df_zone_id_tmp.sqlContext.sql("SELECT max(avg_value) avg_value_neighbour_max, avg(avg_value) avg_value_neighbour_mean " +
          "FROM df_zone_id_v " +
          "GROUP BY zone_id, interest_id")
    */
    //    df_zone_id.printSchema()
    //
    //    println("-------------df_zone_id---------------------SHOW ")
    //    df_zone_id.show(30, false)
    //    println("-------------df_zone_id---------------------COUNT " + df_zone_id.count())
    //
    df_zone_id.coalesce(48).write.mode("overwrite").parquet("C:\\Programming\\vtb\\process_mailru\\geo_tmp\\tmp_" + mode + "_mailru_neighbour_ready")

    println("  Processing prepare_neigbour_aggregates phase finished...." + "\n")
    println("Start: " + start_t3 + "      End: " + Calendar.getInstance().getTime() + "\n")
    // ------------------prepare_neigbour_aggregates--------------------------------------------------------------------------



    // ------------------prepare_city_relative_aggregates--------------------------------------------------------------------------

    println(" Processing prepare_city_relative_aggregates phase started...")
    println(Calendar.getInstance().getTime())
    val start_t4 = Calendar.getInstance().getTime()

    val prim_zone_city = sqlContext.read.parquet("C:\\Programming\\vtb\\process_mailru\\geo_tmp\\tmp_" + mode + "_mailru_primary_zone_city")
    val city_agg = sqlContext.read.parquet("C:\\Programming\\vtb\\process_mailru\\geo_tmp\\tmp_" + mode + "_mailru_agg_city")

    val primary_relative_agg = prim_zone_city.join(
      broadcast(city_agg), prim_zone_city("city_id") === city_agg("city_id") && prim_zone_city("interest_id") === city_agg("interest_id"), "inner").select(
      prim_zone_city("zone_id"),
      prim_zone_city("city_id"),
      prim_zone_city("interest_id"),
      prim_zone_city("avg_value"),
      round((prim_zone_city("avg_value") / city_agg("avg_value_city_relative")), 3).cast("float").alias("avg_value_city_relative"))
      //.withColumn("mzone_id", col("zone_id") % 8)
      //.repartition(400,$"mzone_id")

//    primary_relative_agg.show(20, false)
//    primary_relative_agg.printSchema()
//    primary_relative_agg.explain()
//    println("--------- primary_relative_agg  COUNT: " + primary_relative_agg.count())

    primary_relative_agg.coalesce(48).write.mode("overwrite").parquet(
      "C:\\Programming\\vtb\\process_mailru\\geo_tmp\\tmp_" + mode + "_mailru_primary_relative_agg")


    println("  Processing prepare_city_relative_aggregates phase finished...." + "\n")
    println("Start: " + start_t4 + "      End: " + Calendar.getInstance().getTime() + "\n")
   // ------------------prepare_city_relative_aggregates--------------------------------------------------------------------------


   // ------------------prepare_final_dataset_before_pivot--------------------------------------------------------------------------

    println(" Processing prepare_final_dataset_before_pivot, mailru combine phase started...")
    println(Calendar.getInstance().getTime())
    val start_t5 = Calendar.getInstance().getTime()

    val df_neighbour = sqlContext.read.parquet("C:\\Programming\\vtb\\process_mailru\\geo_tmp\\tmp_" + mode + "_mailru_neighbour_ready")
      //.withColumn("mzone_id", col("zone_id") % 8)
      //.repartition(400,$"mzone_id")

    //удалить строку ниже
    //val primary_relative_agg = sqlContext.read.parquet("C:\\Programming\\vtb\\process_mailru\\geo_tmp\\tmp_" + mode + "_mailru_primary_relative_agg")

    val df_final_step1 = df_neighbour.join(
      primary_relative_agg, df_neighbour("zone_id") === primary_relative_agg("zone_id") &&
        df_neighbour("interest_id") === primary_relative_agg("interest_id"), "full_outer").select(
      df_neighbour("zone_id").alias("n_zone_id"),
      df_neighbour("interest_id").alias("n_interest_id"),
      primary_relative_agg("zone_id").alias("p_zone_id"),
      primary_relative_agg("city_id"),
      primary_relative_agg("interest_id").alias("p_interest_id"),
      primary_relative_agg("avg_value"),
      primary_relative_agg("avg_value_city_relative"),
      df_neighbour("avg_value_neighbour_max"),
      df_neighbour("avg_value_neighbour_mean")
    ).withColumn("zone_id", when($"n_zone_id".isNull, $"p_zone_id").otherwise($"n_zone_id"))
      .withColumn("interest_id", when($"n_interest_id".isNull, $"p_interest_id").otherwise($"n_interest_id"))

    df_final_step1.coalesce(48).write.mode("overwrite").parquet("C:\\Programming\\vtb\\process_mailru\\geo_tmp\\tmp_" + mode + "_df_final_step1")

//    df_final_step1.show(20, false)
//    df_final_step1.printSchema()
//    df_final_step1.explain()
//    println("--------- df_final_step1  COUNT: " + df_final_step1.count())

    //val int_x_fdl = sqlContext.read.parquet("C:\\Programming\\vtb\\process_mailru\\geo\\int_x_fld")
//
//    int_x_fdl.show(20, false)
//    int_x_fdl.printSchema()
//    int_x_fdl.explain()
//    println("--------- int_x_fdl  COUNT: " + int_x_fdl.count())
//
    println("Start: " + start_t5 + "      End: " + Calendar.getInstance().getTime() + "\n")


    val df_final_step2 = sqlContext.read.parquet("C:\\Programming\\vtb\\process_mailru\\geo_tmp\\tmp_" + mode + "_df_final_step1")

    val df_before_pivot = df_final_step2.join(
      //broadcast(int_x_fdl), df_final_step1("interest_id")  === int_x_fdl("interest_id"), "right").select(
      int_x_fdl, df_final_step2("interest_id")  === int_x_fdl("interest_id"), "right").select(
      df_final_step2("zone_id"),
      df_final_step2("city_id"),
      int_x_fdl("fieldname_cd").alias("interest_name"),
      df_final_step2("avg_value").alias("value"),
      df_final_step2("avg_value_city_relative").alias("city_relative"),
      df_final_step2("avg_value_neighbour_max").alias("neighbour_max"),
      df_final_step2("avg_value_neighbour_mean").alias("neighbour_mean"))

//    df_before_pivot.show(20, false)
//    df_before_pivot.printSchema()
//    df_before_pivot.explain()
//    println("--------- df_before_pivot  COUNT: " + df_before_pivot.count())

    //val start_t5 = Calendar.getInstance().getTime()
    df_before_pivot.coalesce(48).write.mode("overwrite").parquet("C:\\Programming\\vtb\\process_mailru\\geo_tmp\\tmp_" + mode + "_mailru")

    println("  Processing prepare_final_dataset_before_pivot, mailru combine phase finished...." + "\n")
    println("Start: " + start_t5 + "      End: " + Calendar.getInstance().getTime() + "\n")
*/

   // ------------------prepare_final_dataset_before_pivot--------------------------------------------------------------------------



    // ------------------process_pivot--------------------------------------------------------------------------

    println(" Processing process_pivot phase started...")
    println(Calendar.getInstance().getTime())
    val start_t6 = Calendar.getInstance().getTime()

    val df_before_pivot_2 = sqlContext.read.parquet("C:\\Programming\\vtb\\process_mailru\\geo_tmp\\tmp_" + mode + "_mailru")

    val df_pivot = df_before_pivot_2.groupBy("zone_id").pivot("interest_name").agg(
      expr("sum(value)").alias("value"), expr("sum(city_relative)").alias("city_relative"),
      expr("sum(neighbour_max)").alias("neighbour_max"), expr("sum(neighbour_mean)").alias("neighbour_mean"))

     df_pivot.coalesce(48).write.mode("overwrite").parquet("C:\\Programming\\vtb\\process_mailru\\geo_tmp\\tmp_" + mode + "_final_mailru")

    println("  Processing process_pivot phase finished...." + "\n")
    println("Start: " + start_t6 + "      End: " + Calendar.getInstance().getTime() + "\n")


//    println("Start_t1: " + start_t1 + "     Start_t2: " + start_t2 +
//      "     Start_t3: " + start_t3 + "     Start_t4: " + start_t4 + "    Start_t5: " + start_t5 + "     Start_t6: " + start_t6 +  "\n")

    // ------------------process_pivot--------------------------------------------------------------------------


//    val fin = sqlContext.read.parquet("C:\\Programming\\vtb\\process_mailru\\geo_tmp\\tmp_" + mode + "_final_mailru")
//    fin.show(20, false)
//    fin.printSchema()
//    fin.explain()
//    println("--------- df_pivot  COUNT: " + fin.count())



  }



}