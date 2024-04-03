/*
chcp 65001 && spark-shell -i C:\Users\Esdesu\Desktop\JreJre\ETL\HomeWork\ETL\Work#1\Task_4\t4.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
*/
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, collect_list, concat_ws}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import com.typesafe.config.{Config, ConfigFactory}

val config = ConfigFactory.load("C:/Users/Esdesu/Desktop/JreJre/ETL/config.conf")
val password = config.getString("MYSQL_PASSWORD")

val t1 = System.currentTimeMillis()
if(1==1){
    var df1 = spark.read.format("com.crealytics.spark.excel")
            .option("sheetName", "Sheet1")
            .option("useHeader", "false")
            .option("treatEmptyValuesAsNulls", "false")
            .option("inferSchema", "true").option("addColorColumns", "true")
            .option("usePlainNumberFormat","true")
            .option("startColumn", 0)
            .option("endColumn", 99)
            .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")
            .option("maxRowsInMemory", 20)
            .option("excerptSize", 10)
            .option("header", "true")
            .format("excel")
            .load("C:/Users/Esdesu/Desktop/JreJre/ETL/HomeWork/ETL/Work#1/Task_4/t4.xlsx")
            df1.show()
            df1.filter(col("Employee_ID").isNotNull).select("Employee_ID", "Job", "Home_city")
            .write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=" +password)
            .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "testtabl1")
            .mode("overwrite").save()

            val window1 = Window.partitionBy(lit(1)).orderBy(("id")).rowsBetween(Window.unboundedPreceding, Window.currentRow)
            df1.withColumn("id", monotonicallyIncreasingId())
            .withColumn("Employee_ID", when(col("Employee_ID").isNull, last("Employee_ID", ignoreNulls = true).over(window1)).otherwise(col("Name")))
            .orderBy("id").drop("id", "Employee_ID", "Job", "Home_city")
            .write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=" +password)
            .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "testtabl2")
            .mode("overwrite").save()

            // val window3 = Window.partitionBy(lit(1)).orderBy(("id")).rowsBetween(Window.unboundedPreceding, Window.currentRow)
            // df1.withColumn("id", monotonicallyIncreasingId())
            // .withColumn("Employee_ID", when(col("Employee_ID").isNull, last("Employee_ID", ignoreNulls = true).over(window1)).otherwise(col("Name")))
            // .orderBy("id").drop("id", "Employee_ID", "City_code", "Name")
            // .write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=")
            // .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "testtabl2")
            // .mode("overwrite").save()

            println("Go Horse Go")
}

val s0 = (System.currentTimeMillis() - t1)/1000
val s = s0 % 60
val m = (s0/60) % 60
val h = (s0/60/60) % 24
println("%02d:%02d:%02d".format(h, m, s))
System.exit(0)
