from re import I
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import regexp_replace

def isNA(df, col):
	df_new = df.withColumn(col, 
		F.when(
				(
					(F.col(col).isNull()) |
					(F.col(col) == "NA")
				), 0
			).otherwise(F.col(col))
		)
	return df_new

def typeChange(df, col, type):
	df_new = df.withColumn(
		col, 
			F.col(col)
			.cast(type)
		)
	return df_new

def pergunta1(df):
	df_without_na = isNA(df, "StockCode")
	df_without_na = isNA(df_without_na, "UnitPrice")
	df_without_na = isNA(df_without_na, "Quantity")
	df_without_na = df_without_na.withColumn('UnitPrice', regexp_replace('UnitPrice', ',', '.'))
	df_final = typeChange(df_without_na, "UnitPrice", "float")
	df_final = typeChange(df_without_na, "Quantity", "int")
	df_final_filtered = df_final.select(
		"StockCode", "Quantity", "UnitPrice"
		).where(
			(F.col('StockCode').rlike("^gift_0001")) & 
			(F.col("UnitPrice") > 0)
		)
	# df_final_filtered.show()
	df_final_filtered.groupBy().sum('Quantity').show()

def pergunta2(df):
	df_without_na = isNA(df, "StockCode")
	df_without_na = isNA(df_without_na, "UnitPrice")
	df_without_na = isNA(df_without_na, "Quantity")
	df_without_na = df_without_na.withColumn('UnitPrice', regexp_replace('UnitPrice', ',', '.'))
	df_final = typeChange(df_without_na, "UnitPrice", "float")
	df_final = typeChange(df_without_na, "Quantity", "int")
	df_final = (df_final.withColumn("InvoiceDate", F.lpad(F.col('InvoiceDate'), 16, '0'))
       .withColumn("InvoiceDate",F.to_timestamp(F.col("InvoiceDate"), 'd/M/yyyy HH:mm'))
      )
	df_final_filtered = df_final.select(
		"StockCode", "Quantity", "UnitPrice", "InvoiceDate"
		).where(
			(F.col('StockCode').rlike("^gift_0001")) & 
			(F.col("UnitPrice") > 0)
		)
	df_final_filtered.groupBy(F.month(F.col("InvoiceDate")).alias("meses")).count().orderBy('meses').show()
	

if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Online Retail]"))

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          #.schema(schema_online_retail)
		          .load("/home/spark/capgemini-aceleracao-pyspark/data/online-retail/online-retail.csv"))
	#.print(df.show())
	# pergunta1(df)
	# pergunta2(df)

