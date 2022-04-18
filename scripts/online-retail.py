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

def treatcsv(df):
    df_new = df.withColumn(
        "StockCode",
        F.when(F.col("StockCode") == "PADS", 0).otherwise(F.col("StockCode"))
    )
    df_new = isNA(df_new, 'InvoiceNo')
    df_new = isNA(df_new, 'StockCode')
    df_new = isNA(df_new, 'Quantity')
    df_new = isNA(df_new, 'InvoiceDate')
    df_new = isNA(df_new, 'UnitPrice')
    df_new = isNA(df_new, 'CustomerID')
    df_new = isNA(df_new, 'Country')
    df_new = df_new.withColumn('UnitPrice', regexp_replace('UnitPrice', ',', '.'))
    df_new = typeChange(df_new, "UnitPrice", "float")
    df_new = typeChange(df_new, "Quantity", "int")
    df_new = (df_new.withColumn("InvoiceDate", F.lpad(F.col('InvoiceDate'), 16, '0'))
               .withColumn("InvoiceDate",F.to_timestamp(F.col("InvoiceDate"), 'd/M/yyyy HH:mm')))
    
    return df_new

def pergunta1(df):

	df_final_filtered = df.select(
		"StockCode", "Quantity", "UnitPrice"
		).where(
			(~F.col('InvoiceNo').rlike("^C")) &
			(F.col('StockCode').rlike("^gift_0001")) & 
			(F.col("UnitPrice") > 0) & 
			(F.col("Quantity") > 0)
		)
	# df_final_filtered.show()
	print("Pergunta 1")
	df_final_filtered.groupBy().sum('Quantity').show()

def pergunta2(df):
	df_final_filtered = df.select(
		"StockCode", "Quantity", "UnitPrice", "InvoiceDate"
		).where(
			(~F.col('InvoiceNo').rlike("^C")) &
			(F.col('StockCode').rlike("^gift_0001")) & 
			(F.col("UnitPrice") > 0) &
			(F.col("Quantity") > 0)
		)
	print("Pergunta 2")
	(df_final_filtered.groupBy(F.month(F.col("InvoiceDate"))
	.alias("meses"))
	.count()
	.orderBy('meses', ascending=False)
	.show())
	
def pergunta3(df):
	df_new = df.select("*").where(F.col("StockCode") == "S")
	print("Pergunta 3")
	df_new.groupBy("StockCode").count().show()

def pergunta4(df):
	df_final_filtered = df.select(
		"*"
		).where(
			(~F.col('InvoiceNo').rlike("^C")) &
			(F.col("StockCode") > 0) &
			(F.col("Quantity") > 0) &
			(F.col("UnitPrice") > 0)
		)

	print("Pergunta 4")
	(df_final_filtered.groupBy("StockCode")
	.sum('Quantity')
	.alias("Total Quantity")
	.orderBy('Total Quantity.sum(Quantity)', ascending=False)
	.show())

def pergunta5(df):
	df_final_filtered = df.select(
		"StockCode", "Quantity", "UnitPrice", "InvoiceDate"
			).where(
				(~F.col('InvoiceNo').rlike("^C")) &
				(F.col("StockCode") > 0) &
				(F.col("Quantity") > 0) &
				(F.col("UnitPrice") > 0)
			)

	print("Pergunta 5")
	(df_final_filtered
	.groupBy(
		F.month(F.col("InvoiceDate")).alias("months")
		).sum('Quantity')
		.alias("Total Quantity")
		.orderBy("Total Quantity.sum(Quantity)")
		.show())

def pergunta6(df):

	df_final_filtered = df.select(
		"StockCode", "Quantity", "UnitPrice", "InvoiceDate"
			).where(
				(~F.col('InvoiceNo').rlike("^C")) &
				(F.col("StockCode") > 0) &
				(F.col("Quantity") > 0) &
				(F.col("UnitPrice") > 0)
			)
	print("Pergunta 6")
	(df_final_filtered
	.groupBy(
		F.hour(F.col("InvoiceDate")).alias("hour")
		).sum('UnitPrice')
		.alias("Total UnitPrice")
		.orderBy("Total UnitPrice.sum(UnitPrice)", ascending=False)
		.show())

def pergunta7(df):

	df_final_filtered = df.select(
		"StockCode", "Quantity", "UnitPrice", "InvoiceDate"
			).where(
				(~F.col('InvoiceNo').rlike("^C")) &
				(F.col("StockCode") > 0) &
				(F.col("Quantity") > 0) &
				(F.col("UnitPrice") > 0)
			)
	print("Pergunta 7")
	(df_final_filtered
	.groupBy(
		F.month(F.col("InvoiceDate")).alias("months")
		).sum('UnitPrice')
		.alias("Total UnitPrice")
		.orderBy("Total UnitPrice.sum(UnitPrice)", ascending=False)
		.show())

def pergunta9(df):

	df_final_filtered = df.select(
		"StockCode", "Quantity", "UnitPrice", "InvoiceDate", "Country"
			).where(
				(~F.col('InvoiceNo').rlike("^C")) &
				(F.col("StockCode") > 0) &
				(F.col("Quantity") > 0) &
				(F.col("UnitPrice") > 0)
			)
	print("Pergunta 9")
	(df_final_filtered
	.groupBy(
		F.col('Country').alias("Country")
		).sum("UnitPrice")
		.alias("Total UnitPrice")
		.orderBy("Total UnitPrice.sum(UnitPrice)", ascending=False)
		.show())

def pergunta10(df):
	
	df_final_filtered = df.select(
		"StockCode", "Quantity", "UnitPrice", "InvoiceDate", "Country"
			).where(
				(~F.col('InvoiceNo').rlike("^C")) &
				(F.col("UnitPrice") > 0) &
				(F.col("Quantity") > 0) &
				(F.col("StockCode").rlike("M"))
			)
	print("Pergunta 10")
	(df_final_filtered
	.groupBy(
		F.col('Country').alias("Country")
		).sum("UnitPrice")
		.alias("Total UnitPrice")
		.orderBy("Total UnitPrice.sum(UnitPrice)", ascending=False)
		.show())

def pergunta11(df):
	
	df_final_filtered = df.select(
		"*"
			).where(
				(~F.col('InvoiceNo').rlike("^C")) &
				(F.col("UnitPrice") > 0) &
				(F.col("Quantity") > 0) &
				(F.col("InvoiceNo") > 0)
			)
	df_final_filtered_total_value = df_final_filtered.withColumn(
    "Total Value",
    F.ceil(F.expr("Quantity * UnitPrice"))
    )
	print("Pergunta 11")
	(df_final_filtered_total_value
	.groupBy("InvoiceNo")
		.sum("Total Value")
		.orderBy("sum(Total Value)", ascending=False)
		.show())

def pergunta12(df):
	
	df_final_filtered = df.select(
		"*"
			).where(
				(~F.col('InvoiceNo').rlike("^C")) &
				(F.col("UnitPrice") > 0) &
				(F.col("Quantity") > 0) &
				(F.col("InvoiceNo") > 0)
			)
	df_final_filtered_total_value = df_final_filtered.withColumn(
    "Total Value",
    F.ceil(F.expr("Quantity * UnitPrice"))
    )
	print("Pergunta 12")
	(df_final_filtered_total_value
	.groupBy("InvoiceNo")
		.sum("Quantity")
		.orderBy("sum(Quantity)", ascending=False)
		.show())

def pergunta13(df):
	
	df_final_filtered = df.select(
		"StockCode", "Quantity", "UnitPrice", "InvoiceDate", "Country", "CustomerID"
			).where(
				(~F.col('InvoiceNo').rlike("^C")) &
				(F.col("UnitPrice") > 0) &
				(F.col("Quantity") > 0) &
				(F.col("CustomerID") > 0)
			)
	print("Pergunta 13")
	(df_final_filtered
	.groupBy(
		F.col('CustomerID')
		).count()
		.alias("Total buy")
		.orderBy("Total buy.count", ascending=False)
		.show())

if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Online Retail]"))

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          #.schema(schema_online_retail)
		          .load("/home/spark/capgemini-aceleracao-pyspark/data/online-retail/online-retail.csv"))
	#.print(df.show())
	df_treated = treatcsv(df)
	# pergunta1(df_treated)
	# pergunta2(df_treated)
	# pergunta3(df_treated)
	# pergunta4(df_treated)
	# pergunta5(df_treated)
	# pergunta6(df_treated)
	# pergunta7(df_treated)
	pergunta9(df_treated)
	pergunta10(df_treated)
	pergunta11(df_treated)
	pergunta12(df_treated)
	pergunta13(df_treated)
