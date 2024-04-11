/*
chcp 65001 && spark-shell -i C:\Users\Esdesu\Desktop\JreJre\ETL\HomeWork\ETL\Work#3\Task_5\Task_1_v2.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
*/

import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, collect_list, concat_ws}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import scala.io.Source

sc.setLogLevel("ERROR")

val login = Source.fromFile("C:/Users/Esdesu/Desktop/JreJre/ETL/config.txt").getLines.toList

val t1 = System.currentTimeMillis()

if(1==1){
	var df = spark.read.option("delimiter",",")
		.option("inferSchema", "true")			
		.option("header", "true")
		.format("excel")
		.load("C:/Users/Esdesu/Desktop/JreJre/ETL/HomeWork/ETL/Work#3/Task_5/s3.xlsx")

	val df1 = df
		df1.write.format("jdbc").option("url", login(0))
			.option("driver", login(1)).option("dbtable", "w3t5v2")
			.mode("overwrite").save()
		df1.show()

    val df2 = spark.read.format("jdbc").option("url", login(0))
        .option("driver", login(1))
        .option("dbtable", "w3t5v2")
        .load()

    val df_group = df2.distinct().where(col("fieldname") === "GNAME2")
        .select("objectid", "restime", "fieldvalue")
        .withColumnRenamed("fieldvalue", "Group")
        .withColumn("Destination", lit("1").cast("integer"))

    val df_status = df2.distinct().where(col("fieldname") === "Status")
        .select("objectid", "restime", "fieldvalue")
        .withColumnRenamed("fieldvalue", "Status")

    val df_sg = df2.filter((col("fieldname") isin ("status", "GNAME2")))
        .select("objectid", "restime").distinct()

    val df_inner = df_sg.as("a")
        .join(df_status.as("a1"),col("a.objectid") === col("a1.objectid") && col("a.restime") === col("a1.restime"),"left")
        .join(df_group.as("a2"),col("a.objectid") === col("a2.objectid") && col("a.restime") === col("a2.restime"),"left")
        .select(col("a.objectid"),col("a.restime"),col("a1.Status"),col("a2.Group"),col("a2.Destination"))
        .withColumnRenamed("objectid", "Tiket")
        .withColumnRenamed("restime", "StatusTime")
        .distinct()

    val df_outer = df_inner.select(col("Tiket"),col("StatusTime"),col("Status"),when(row_number().over(Window.partitionBy(col("Tiket"))
        .orderBy(col("StatusTime"))) === 1 && col("Destination").isNull,"").otherwise(col("Group")).alias("Group"),col("Destination"))
    
    val df_result = df_outer.select(col("Tiket"),from_unixtime(col("StatusTime")).alias("StatusTime"),((lead(col("StatusTime"), 1)
        .over(Window.partitionBy(col("Tiket")).orderBy(col("StatusTime"))) - col("StatusTime")) / 3600).alias("Timers"),last(col("Status"), true)
        .over(Window.partitionBy(col("Tiket")).orderBy(col("StatusTime")))
        .alias("Status"),last(col("Group"), true).over(Window.partitionBy(col("Tiket")).orderBy(col("StatusTime")))
        .alias("Group"),col("Destination"))
        .withColumn("Timers", coalesce(col("Timers"), lit(0)))
        .withColumn("Timers", round(col("Timers"), 4))

    df_result.write.format("jdbc").option("url", login(0))
        .option("driver", login(1)).option("dbtable", "w3t5v2a")
		.mode("overwrite").save()
    df_result.show()
    
	println("W")

    val df3 = spark.read.format("jdbc").option("url", login(0))
        .option("driver", login(1))
        .option("dbtable", "w3t5v2a")
        .load()

    val df3_concat = df3.groupBy("Tiket")
        .agg(concat_ws("\r\n", collect_list(concat_ws(", ",when(date_format(col("StatusTime"), "yyyy-MM-dd") === current_date(),date_format(col("StatusTime"), "yyyy-MM-dd HH:mm:ss"))
        .otherwise(date_format(col("StatusTime"), "dd-MM-yyyy HH:mm")),when(col("Status") === "Зарегистрирован", "З —> ")
        .when(col("Status") === "Назначен", "Н —> ")
        .when(col("Status") === "В работе", "ВР —> ")
        .when(col("Status") === "Закрыт", "ЗТ —> ")
        .when(col("Status") === "Исследование ситуации", "ИС —> ")
        .when(col("Status") === "Решен", "Р —> ")
        .otherwise(col("Status")),col("Group"))))
        .alias("new format"))
        .withColumn("Tiket",col("Tiket"))
      

    df3_concat.write.format("jdbc").option("url", login(0))
        .option("driver", login(1)).option("dbtable", "w3t5v2b")
        .mode("overwrite").save()
    df3_concat.show()
    
	println("Work 3, Task 5, Successful Load and Save")

}

val s0 = (System.currentTimeMillis() - t1)/1000
val s = s0 % 60
val m = (s0/60) % 60
val h = (s0/60/60) % 24
println("%02d:%02d:%02d".format(h, m, s))
System.exit(0)
