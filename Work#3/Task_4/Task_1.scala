/*
chcp 65001 && spark-shell -i C:\Users\Esdesu\Desktop\JreJre\ETL\HomeWork\ETL\Work#3\Task_4\Task_1.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
*/

import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, collect_list, concat_ws}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
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
		.load("C:/Users/Esdesu/Desktop/JreJre/ETL/HomeWork/ETL/Work#3/Task_4/s3.xlsx")

	val df1 = df
		df1.write.format("jdbc").option("url", login(0))
			.option("driver", login(1)).option("dbtable", "w3t5")
			.mode("overwrite").save()
		df1.show()

	val df2 = spark.read.format("jdbc").option("url", login(0))
		.option("driver", login(1))
    	.option("query","""
				SELECT `tiket`, FROM_UNIXTIME(`StatusTime`) `StatusTime`, 
					IFNULL((LEAD(`StatusTime`) OVER(PARTITION BY `tiket` ORDER BY `StatusTime`) - `StatusTime`) / 3600, 0)  AS Timers,
					CASE WHEN `status` IS NULL THEN @PREF1
					ELSE @PREF1:=`status`
					END `status`,
					CASE WHEN `group` IS NULL THEN @PREF2
					ELSE @PREF2:=`group`
					END `group`
				FROM 
				(SELECT `tiket`, `StatusTime`, `status`, 
				IF(ROW_NUMBER() OVER(PARTITION BY `tiket` ORDER BY `StatusTime`) = 1 AND 'Destination' IS NULL, '', `group`) `group`
				FROM
				(SELECT DISTINCT a.objectid `tiket`, a.restime `StatusTime`, `status`, `group`, 'Destination', 
				(SELECT @PREF1:=''), 
				(SELECT @PREF2:='') 
				FROM 
				(SELECT DISTINCT a.objectid, a.restime FROM spark.w3t5 AS a
				WHERE fieldname IN ('status' AND 'GNAME2')) AS a
				LEFT JOIN 
					(SELECT DISTINCT `objectid`, `restime`, `fieldvalue` 'status' FROM spark.w3t5
					WHERE fieldname IN ('status')) AS a1
				ON a.objectid = a1.objectid AND a.restime = a1.restime
				LEFT JOIN 
					(SELECT DISTINCT `objectid`, `restime`, `fieldvalue` 'group', 1 'Destination' FROM spark.w3t5
					WHERE fieldname IN ('GNAME2')) AS a2
				ON a.objectid = a2.objectid AND a.restime = a2.restime) AS b1
				ORDER BY 1, 2) AS b2
			""")
		.load()
		
		df2.write.format("jdbc").option("url", login(0))
			.option("driver", login(1)).option("dbtable", "w3t5a")
			.mode("overwrite").save()
		df2.show()

	val df3 = spark.read.format("jdbc").option("url", login(0))		
		.option("driver", login(1))
		.option("query","""
			SELECT `tiket`, GROUP_CONCAT(' ', 
			CASE 
				WHEN DATE(`StatusTime`) = CURDATE() THEN DATE_FORMAT(`StatusTime`, '%Y-%m-%d %H:%i:%s')
			ELSE DATE_FORMAT(`StatusTime`, '%d-%m-%Y %H:%i')
			END, 
			', ', 
			CASE 
				WHEN `status` = 'Зарегистрирован' THEN 'З'
				WHEN `status` = 'Назначен' THEN 'Н'
				WHEN `status` = 'В работе' THEN 'ВР'
				WHEN `status` = 'Закрыт' THEN 'ЗТ'
				WHEN `status` = 'Исследование ситуации' THEN 'ИС'
				WHEN `status` = 'Решен' THEN 'Р'
			ELSE `status`
			END,
			' -> ', `group`, '. ' SEPARATOR '\r\n') AS 'new format'
			FROM spark.w3t5a
			GROUP BY 1
			""")
		.load()
		
		df3.write.format("jdbc").option("url", login(0))
			.option("driver", login(1)).option("dbtable", "w3t5b")
			.mode("overwrite").save()
		df3.show()

	println("Work 3, Task 5, Successful Load and Save")
}

val s0 = (System.currentTimeMillis() - t1)/1000
val s = s0 % 60
val m = (s0/60) % 60
val h = (s0/60/60) % 24
println("%02d:%02d:%02d".format(h, m, s))
System.exit(0)
