/*
chcp 65001 && spark-shell -i C:\Users\Esdesu\Desktop\JreJre\ETL\Working\W2\s2.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
*/

import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, collect_list, concat_ws}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
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

var misqlCon = s"jdbc:mysql://localhost:3306/spark?user=$login&password=$password"
var driver = "com.mysql.cj.jdbc.Driver"

val t1 = System.currentTimeMillis()

if(1==11){
	var df1 = spark.read.option("delimiter",",")
			.option("header", "true")
			//.option("encoding", "windows-1251")
			.csv("C:/Users/Esdesu/Desktop/JreJre/ETL/Working/W2/s2_data.csv")
			df1=df1
			.withColumn("children",col("children").cast("int"))
			.withColumn("dob_years",col("dob_years").cast("int"))
			.withColumn("family_status_id",col("family_status_id").cast("int"))
			.withColumn("education_id",col("education_id").cast("int"))
			.withColumn("debt",col("debt").cast("int"))
			.withColumn("education",lower(col("education")))
			
			.withColumn("days_employed",col("days_employed").cast("float")) 
			.withColumn("total_income",col("total_income").cast("float")).dropDuplicates()
			.withColumn("purpose_category", 
				when(col("purpose").like("%авто%"),"операции с автомобилем")
				.when(col("purpose").like("%недвиж%")||col("purpose").like("%жиль%"),"операции с недвижимостью")
				.when(col("purpose").like("%свадьб%"),"проведение свадьбы")
				.when(col("purpose").like("%образ%"),"получение образования")
				.otherwise("Unknown")
			)
		.withColumn("total_income2",
		when(col("total_income").isNotNull,col("total_income"))
		.otherwise(avg("total_income").over(Window.partitionBy("income_type").orderBy("income_type"))))
		.withColumn("total_income2",col("total_income2").cast("float"))
			df1.write.format("jdbc").option("url", misqlCon)
			.option("driver", driver).option("dbtable", "tasketl2a")
			.mode("overwrite").save()
			df1.show()

val s = df1.columns.map(c => sum(col(c).isNull.cast("integer")).alias(c))
val df2 = df1.agg(s.head, s.tail:_*)
val t = df2.columns.map(c => df2.select(lit(c).alias("col_name"), col(c).alias("null_count")))
val df_agg_col = t.reduce((df1, df2) => df1.union(df2))
df_agg_col.show()
}

import java.sql._;

def sqlexecute(sql:String) = {
    var conn:Connection=null;
    var stmt:Statement=null;
    try {
        Class.forName(driver)
        conn=DriverManager.getConnection(misqlCon)
        stmt=conn.createStatement();
        stmt.executeUpdate(sql);
        println(sql+" complete")
    } catch {
        case e: Exception =>println(e);
    }
}

def checktable(scheme:String,table:String) = {
    var conn:Connection=null;
    var dmd:DatabaseMetaData=null;
    var rs:ResultSet=null;
    try {
        Class.forName(driver)
        conn=DriverManager.getConnection(misqlCon)
        dmd=conn.getMetaData();
        rs=dmd.getTables(scheme,null,table,null)
        rs.next()
        println(rs.getString("TABLE_NAME")+" exists")
    } catch {
        case e: Exception =>println(table+" doesn't exist");
    }
}

val ct = """CREATE TABLE IF NOT EXISTS `task04` (
	`Отдел` TEXT NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci',
	`Начальник` TEXT NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci',
	`Сотрудник` TEXT NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci'
)
COLLATE='utf8mb4_0900_ai_ci'
ENGINE=InnoDB
;"""

val data = Seq("Кадры", "Риторта", "Микс")
sqlexecute(s"insert into task04 VALUES ('${data(0)}','${data(1)}','${data(2)}')")

val df1 = sc.parallelize(List( ("Бухгалтерия", "Вася", "Петя") )).toDF("Отдел", "Начальник", "Сотрудник")
df1.show()

df1.write.format("jdbc").option("url", misqlCon)
        .option("driver", driver).option("dbtable", "task04")
        .mode("append").save()

val s0 = (System.currentTimeMillis() - t1)/1000
val s = s0 % 60
val m = (s0/60) % 60
val h = (s0/60/60) % 24
println("%02d:%02d:%02d".format(h, m, s))
System.exit(0)
