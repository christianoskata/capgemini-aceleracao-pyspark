from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
 
def isNA(df, col):
	df_new = df.withColumn(col, 
		F.when(
				(
					(F.col(col).isNull()) |
					(F.col(col) == "?")
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

	df_new = isNA(df, "county")
	df_new = isNA(df_new, "community")
	df_new = isNA(df_new, "communityname")
	df_new = isNA(df_new, "population")
	df_new = isNA(df_new, "PolicPerPop")
	df_new = isNA(df_new, "PolicAveOTWorked")
	df_new = isNA(df_new, "LandArea")
	df_new = isNA(df_new, "PolicCars")
	df_new = isNA(df_new, "PolicOperBudg")
	df_new = isNA(df_new, "LemasPctPolicOnPatr")
	df_new = isNA(df_new, "PolicBudgPerPop")

	df_new = typeChange(df_new, "county", "int")
	df_new = typeChange(df_new, "community", "int")
	df_new = typeChange(df_new, "population", "float")
	df_new = typeChange(df_new, "PolicPerPop", "float")
	df_new = typeChange(df_new, "LandArea", "float")
	df_new = typeChange(df_new, "PolicCars", "float")
	df_new = typeChange(df_new, "PolicOperBudg", "float")
	df_new = typeChange(df_new, "LemasPctPolicOnPatr", "float")
	df_new = typeChange(df_new, "PolicBudgPerPop", "float")

	df_filtered = df_new.select(
		"county", 
		"community", 
		"communityname", 
		"population", 
		"PolicPerPop",
		"PolicAveOTWorked", 
		"LandArea", 
		"PolicCars", 
		"PolicOperBudg",
		"LemasPctPolicOnPatr", 
		"PolicBudgPerPop").where(
				(F.col("county") > 0) & 
				(F.col("community") > 0) & 
				(F.col("population") > 0) & 
				(F.col("PolicPerPop") > 0) & 
				(F.col("PolicAveOTWorked") > 0) & 
				(F.col("LandArea") > 0) & 
				(F.col("PolicCars") > 0) & 
				(F.col("PolicOperBudg") > 0) & 
				(F.col("LemasPctPolicOnPatr") > 0) & 
				(F.col("PolicBudgPerPop") > 0) 
			)

	df_filtered.orderBy("PolicBudgPerPop", ascending=False).show()

def pergunta2(df):

	df2 = df.select("county", 
					"community", 
					"communityname", 
                	"population", 
					"ViolentCrimesPerPop")

	df_new = isNA(df2, "county")
	df_new = isNA(df_new, "community")
	df_new = isNA(df_new, "communityname")
	df_new = isNA(df_new, "population")
	df_new = isNA(df_new, "ViolentCrimesPerPop")

	df_new = typeChange(df_new, "county", "int")
	df_new = typeChange(df_new, "community", "int")
	df_new = typeChange(df_new, "population", "float")
	df_new = typeChange(df_new, "ViolentCrimesPerPop", "float")

	df_filtered = (df_new.select(
		"county", "community", "communityname", "population", "ViolentCrimesPerPop")
			.where(
			(F.col("county") > 0) & 
			(F.col("community") > 0) & 
			(F.col("population") > 0) & 
			(F.col("ViolentCrimesPerPop") > 0)
			)
		)
	df_filtered.orderBy("ViolentCrimesPerPop","population", ascending=False).show()

def pergunta3(df):

	df3 = df.select("county", 
					"community", 
					"communityname", 
                	"population")

	df_new = isNA(df3, "county")
	df_new = isNA(df_new, "community")
	df_new = isNA(df_new, "communityname")
	df_new = isNA(df_new, "population")

	df_new = typeChange(df_new, "county", "int")
	df_new = typeChange(df_new, "community", "int")
	df_new = typeChange(df_new, "population", "float")

	df_filtered = (df_new.select(
		"county", "community", "communityname", "population")
			.where(
				(F.col("county") > 0) & 
				(F.col("community") > 0) & 
				(F.col("population") > 0)
			)
		)
	df_filtered.orderBy("population", ascending=False).show()

if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Communities & Crime]"))

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          #.schema(schema_communities_crime)
		          .load("/home/spark/capgemini-aceleracao-pyspark/data/communities-crime/communities-crime.csv"))
	# print(df.show())
	# pergunta1(df)
	# pergunta2(df)
	pergunta3(df)

