/*
chcp 65001 && spark-shell -i C:\Users\Esdesu\Desktop\JreJre\ETL\HomeWork\ETL\Work#1\Task_4\W1T4.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
*/

import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, collect_list, concat_ws}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import java.io.{File, FileInputStream}
import java.util.Properties

val filePath = "C:/Users/Esdesu/Desktop/JreJre/ETL/config.properties"
val prop = new Properties()
val file = new File(filePath)
val fis = new FileInputStream(file)
prop.load(fis)

val login = prop.getProperty("login")
val password = prop.getProperty("password")

var sqlCoun = s"jdbc:mysql://localhost:3306/spark?user=$login&password=$password"
var driver = "com.mysql.cj.jdbc.Driver"

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
        .load("C:/Users/Esdesu/Desktop/JreJre/ETL/HomeWork/ETL/Work#1/Task_4/w1t4.xlsx")
    df1.show()

        df1.filter(col("Employee_ID").isNotNull).select("Employee_ID", "Job_code")
            .write.format("jdbc").option("url", sqlCoun)
            .option("driver", driver).option("dbtable", "W1T4a")
            .mode("overwrite").save()

    val nf2 = Window.partitionBy(lit(1)).orderBy(("id")).rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val df2 = df1.withColumn("id", monotonicallyIncreasingId())

        df2.withColumn("Employee_ID", when(col("Employee_ID").isNull, last("Employee_ID", ignoreNulls = true).over(nf2)).otherwise(col("Employee_ID")))
            .withColumn("table", lit("W1T4b"))

            .orderBy("id").drop("id", "Job_Code", "Job")
            .filter(col("table") === "W1T4b")
            .dropDuplicates()
            .drop("table")
            .write.format("jdbc").option("url", sqlCoun)
            .option("driver", driver).option("dbtable", "W1T4b")
            .mode("overwrite").save()

        df2.withColumn("table", lit("W1T4c"))
            .orderBy("id").drop("id", "Employee_ID", "Name", "City_code", "Home_city")
            .filter(col("table") === "W1T4c")
            .dropDuplicates()
            .drop("table")
            .write.format("jdbc").option("url", sqlCoun)
            .option("driver", driver).option("dbtable", "W1T4c")
            .mode("overwrite").save()

    val nf3 = Window.partitionBy(lit(1)).orderBy(("id")).rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val df3 = df1.withColumn("id", monotonicallyIncreasingId())

        df3.withColumn("Employee_ID", when(col("Employee_ID").isNull, last("Employee_ID", ignoreNulls = true).over(nf2)).otherwise(col("Employee_ID")))
            .withColumn("table", lit("W1T4b"))

            .orderBy("id").drop("id", "Job_Code", "Job", "Home_city")
            .filter(col("table") === "W1T4b")
            .dropDuplicates()
            .drop("table")
            .write.format("jdbc").option("url", sqlCoun)
            .option("driver", driver).option("dbtable", "W1T4b")
            .mode("overwrite").save()

        df3.withColumn("table", lit("W1T4c"))
            .orderBy("id").drop("id", "Employee_ID", "Name", "City_code", "Home_city")
            .filter(col("table") === "W1T4c")
            .dropDuplicates()
            .drop("table")
            .write.format("jdbc").option("url", sqlCoun)
            .option("driver", driver).option("dbtable", "W1T4c")
            .mode("overwrite").save()

        df3.withColumn("table", lit("W1T4d"))
            .orderBy("id").drop("id", "Employee_ID", "Name", "Job_Code", "Job")
            .filter(col("table") === "W1T4d")
            .dropDuplicates()
            .drop("table")
            .write.format("jdbc").option("url", sqlCoun)
            .option("driver", driver).option("dbtable", "W1T4d")
            .mode("overwrite").save()

    println("Work 1, Task 4, Successful Load and Save")
}

val s0 = (System.currentTimeMillis() - t1)/1000
val s = s0 % 60
val m = (s0/60) % 60
val h = (s0/60/60) % 24
println("%02d:%02d:%02d".format(h, m, s))
System.exit(0)
