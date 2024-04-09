/*
chcp 65001 && spark-shell -i C:/Users/Esdesu/Desktop/JreJre/ETL/HomeWork/ETL/Work#3/Task_5/Task_5.scala" --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
*/

import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, collect_list, concat_ws}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import scala.io.Source

sc.setLogLevel("ERROR")

val lines = Source.fromFile("C:/Users/Esdesu/Desktop/JreJre/ETL/config.txt").getLines.toList
println("test "+lines(0))

val t1 = System.currentTimeMillis()

var df = spark.read.option("delimiter",",")
		.option("header", "true")
		.csv("C:/Users/Esdesu/Desktop/JreJre/ETL/HomeWork/ETL/Work#3/Task_5/s3.xlsx")
	df.show()

// val df1 = df
// 		df1.write.format("jdbc").option("url", misqlCon)
// 			.option("driver", driver).option("dbtable", "w2t1")
// 			.mode("overwrite").save()
// 		df1.show()

println("Work 3, Task 5, Done")

val s0 = (System.currentTimeMillis() - t1)/1000
val s = s0 % 60
val m = (s0/60) % 60
val h = (s0/60/60) % 24
println("%02d:%02d:%02d".format(h, m, s))
System.exit(0)