package red_code

import java.io.PrintWriter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

import java.io._
import org.apache.spark.sql.expressions.Window
import org.json4s.JsonDSL.int2jvalue
import spire.random.Random.int

object redcode {


    def main(args: Array[String]): Unit = {
      // 获取开始时间
      val startTime = System.currentTimeMillis()
      //建立Spark连接
      val spark = SparkSession.builder().appName("redcode").master("local[*]").getOrCreate()
      val sc = spark.sparkContext
      // 将txt文件按照csv格式读入
      val cdinfo = spark.read.csv("cdinfo.txt")

      val infected = spark.read.csv("infected.txt")
      val infected_tlp=infected.select("_c0").rdd.map(row => row(0)).collect.toList.distinct

      // 找出感染的基站
      val filtered = cdinfo.filter(cdinfo("_c3").isin(infected_tlp:_*))
     // val infected_base_num_ori=filtered.select("_c0").rdd.map(row => row(0)).collect.toList

      val infected_base_start = filtered.filter(col("_c2")===1).select("_c1").rdd.map(row => row(0)).collect.toList
      val infected_base_end = filtered.filter(col("_c2")===2).select("_c1").rdd.map(row => row(0)).collect.toList


      // 记录进入基站时间
     // val in_infected_base = filtered.filter(col("_c2")===1).orderBy("_c0","_c3").withColumnRenamed("_c1", "in_time")
      //val infected_base_start = in_infected_base.select( "in_time").rdd.map(row => row(0)).collect.toList
      val infected_base_num=filtered.filter(col("_c2")===1).select("_c0").rdd.map(row => row(0)).collect.toList
      //记录出基站时间
      //val out_infected_base = filtered.filter(col("_c2")===2).orderBy("_c0","_c3").withColumnRenamed("_c1", "out_time")
     // val infected_base_end = out_infected_base.select("out_time").rdd.map(row => row(0)).collect.toList

      //      val in_and_out_base_time=filtered.groupBy("_c0","_c3").agg(collect_list("_c1").as("time"))
//      in_and_out_base_time.show(false)
//      in_and_out_base_time.collect().foreach(println)
      //将进出站时间进行连接
      //val in_and_out_base_time = in_infected_base.join(out_infected_base, Seq("_c0", "_c3"), "inner").orderBy("_c0").drop("_c2")

      // 将重复的进入时间删去
     // in_and_out_base_time.dropDuplicates("_c0","_c3","in_time").drop("_c3")
      //filtered.coalesce(1).write.mode("overwrite").csv("filtered.csv")

      // 使用列表记录基站
//      val infected_base_num=in_and_out_base_time.select("_c0").rdd.map(row => row(0)).collect.toList
      // writeListToFile(infected_base_num, "infected_base_num.txt")


      //

//      def writeListToFile(list: List[Any], filePath: String): Unit = {
//        val writer = new PrintWriter(new FileOutputStream(filePath, true))
//        list.foreach(item => writer.println(item.toString))
//        writer.close()
//        println(s"List 已成功写入到 $filePath 文件中")
//      }

      //writeListToFile(infected_base_start, "infected_base_start.txt")
      //writeListToFile(infected_base_end, "infected_base_end.txt")
      // 进入过被感染的基站的人
      val infected_people=cdinfo.filter(cdinfo("_c0").isin(infected_base_num:_*))
      val may_infected = infected_people.groupBy("_c0","_c3").agg(collect_list("_c1").as("time"))

      // 使用UDF（用户自定义函数）过滤
      def is_infected( r:Row ) : Boolean = {
        for (i <- 0 to (infected_base_num.length-1))
        {
          var j = 0
          while ((j <= r.getAs[Seq[String]]("time").length - 2) && (r.getAs("_c0") == infected_base_num(i))) {

            if (((r.getAs[Seq[String]]("time"))(j).toLong <= infected_base_end(i).toString.toLong) &&
              ((r.getAs[Seq[String]]("time"))(j + 1).toLong >= infected_base_start(i).toString.toLong)) {
              return true
            }
            else{j += 2}
          }

        }
        return false;
      }
      // 注册用户自定义函数
      spark.udf.register("is_infected",is_infected _)
      val final_infected = may_infected.filter(callUDF("is_infected",struct(may_infected.columns.map(may_infected(_)) : _*)))
      final_infected.distinct()



      // 将结果写入txt文件
//      val basic_task=infected_people.select(("_c3")).distinct().sort("_c3")
//      basic_task.coalesce(1).write.mode("overwrite").text("infected_people.txt")
      val final_task= final_infected.select(("_c3")).distinct().sort("_c3")
      final_task.coalesce(1).write.mode("overwrite").text("remark13.txt")
      //获取结束时间
     val endTime = System.currentTimeMillis()
      //计算时间差
      val duration = (endTime - startTime) / 1000
      println(s"start: $startTime")
      println(s"end: $endTime")
      println(s"Execution time: $duration s")
    }

}
//
//&&((r.getAs[String]("out_time").toLong-r.getAs[String]("in_time").toLong)>=(infected_base_end(i).toString.toLong-infected_base_start(i).toString.toLong))