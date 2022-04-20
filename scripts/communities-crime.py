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
				), None
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
    df_new = df.select(
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
        "PolicBudgPerPop",
        "ViolentCrimesPerPop",
        "racepctblack",
        "racePctWhite", 
        "racePctAsian", 
        "racePctHisp",
        "perCapInc",
        "agePct12t21",
        "PctPolicWhite",
        "medFamInc"
    )
    df_new = isNA(df_new, "county")
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
    df_new = isNA(df_new, "ViolentCrimesPerPop")
    df_new = isNA(df_new, "racepctblack")
    df_new = isNA(df_new, "racePctWhite")
    df_new = isNA(df_new, "racePctAsian")
    df_new = isNA(df_new, "racePctHisp")
    df_new = isNA(df_new, "perCapInc")
    df_new = isNA(df_new, "agePct12t21")
    df_new = isNA(df_new, "PctPolicWhite")
    df_new = isNA(df_new, "medFamInc")
    
    df_new = typeChange(df_new, "county", "int")
    df_new = typeChange(df_new, "community", "int")
    df_new = typeChange(df_new, "population", "float")
    df_new = typeChange(df_new, "PolicPerPop", "float")
    df_new = typeChange(df_new, "PolicAveOTWorked", "float")
    df_new = typeChange(df_new, "LandArea", "float")
    df_new = typeChange(df_new, "PolicCars", "float")
    df_new = typeChange(df_new, "PolicOperBudg", "float")
    df_new = typeChange(df_new, "LemasPctPolicOnPatr", "float")
    df_new = typeChange(df_new, "PolicBudgPerPop", "float")
    df_new = typeChange(df_new, "ViolentCrimesPerPop", "float")
    df_new = typeChange(df_new, "racepctblack", "float")
    df_new = typeChange(df_new, "racePctWhite", "float")
    df_new = typeChange(df_new, "racePctAsian", "float")
    df_new = typeChange(df_new, "racePctHisp", "float")
    df_new = typeChange(df_new, "perCapInc", "float")
    df_new = typeChange(df_new, "agePct12t21", "float")
    df_new = typeChange(df_new, "PctPolicWhite", "float")
    df_new = typeChange(df_new, "medFamInc", "float")
    
    return df_new

def pergunta1(df):

	print("Pergunta 1")

	df_filtered = df.select(
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

	print("Pergunta 2")

	df_filtered = (df.select(
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

	print("Pergunta 3")

	df_filtered = (df.select(
		"county", "community", "communityname", "population")
			.where(
				(F.col("county") > 0) & 
				(F.col("community") > 0) & 
				(F.col("population") > 0)
			)
		)
	df_filtered.orderBy("population", ascending=False).show()

def pergunta4(df):

	print("Pergunta 4")

	df_filtered = df.select(
		"county", "community", "communityname", "population", "racepctblack").where(
		(F.col("county") > 0) & 
		(F.col("community") > 0) & 
		(F.col("population") > 0) & 
		(F.col("racepctblack") > 0)
	)
	df_filtered.orderBy("racepctblack","population", ascending=False).show()

def pergunta5(df):

	print("Pergunta 5")

	df_filtered = df_new.select(
		"county", "community", "communityname", "population", "perCapInc").where(
		(F.col("county") > 0) & 
		(F.col("community") > 0) & 
		(F.col("population") > 0) & 
		(F.col("perCapInc") > 0)
		)

	df_filtered.orderBy("perCapInc","population", ascending=False).show()

def pergunta6(df):

	print("Pergunta 6")

	df_filtered = df.select(
		"county", "community", "communityname", "population", "agePct12t21").where(
		(F.col("county") > 0) & 
		(F.col("community") > 0) & 
		(F.col("population") > 0) & 
		(F.col("agePct12t21") > 0)
	)

	df_filtered.orderBy("agePct12t21","population", ascending=False).show()

def pergunta7(df):

	print("Pergunta 7")
	
	df_filtered = df.select(
		"county", "community", "communityname", "population", "ViolentCrimesPerPop", "PolicBudgPerPop").where(
		(F.col("county") > 0) & 
		(F.col("community") > 0) & 
		(F.col("ViolentCrimesPerPop") > 0) & 
		(F.col("population") > 0) & 
		(F.col("PolicBudgPerPop") > 0)
	)

	print(df_filtered.stat.corr("ViolentCrimesPerPop","PolicBudgPerPop"))

def pergunta8(df):

	print("Pergunta 8")
	
	df_filtered = df.select(
		"county", "community", "communityname", "population", "PctPolicWhite", "PolicBudgPerPop").where(
		(F.col("county") > 0) & 
		(F.col("community") > 0) & 
		(F.col("PctPolicWhite") > 0) & 
		(F.col("population") > 0) & 
		(F.col("PolicBudgPerPop") > 0)
	)

	print(df_filtered.stat.corr("PolicBudgPerPop","PctPolicWhite"))

def pergunta9(df):

	print("Pergunta 9")
	
	df_filtered = df.select(
		"county", "community", "communityname", "population", "PolicBudgPerPop").where(
		(F.col("county") > 0) & 
		(F.col("community") > 0) & 
		(F.col("population") > 0) & 
		(F.col("PolicBudgPerPop") > 0)
	)

	print(df_filtered.stat.corr("PolicBudgPerPop","population"))

def pergunta10(df):

	print("Pergunta 10")
	
	df_filtered = df.select(
		"county", "community", "communityname", "population", "ViolentCrimesPerPop").where(
		(F.col("county") > 0) & 
		(F.col("community") > 0) & 
		(F.col("population") > 0) & 
		(F.col("ViolentCrimesPerPop") > 0)
	)

	print(df_filtered.stat.corr("ViolentCrimesPerPop","population"))

def pergunta11(df):

	print("Pergunta 11")
	
	df_filtered = df.select(
		"county", "community", "communityname", "population", "medFamInc", "ViolentCrimesPerPop").where(
		(F.col("county") > 0) & 
		(F.col("community") > 0) & 
		(F.col("medFamInc") > 0) & 
		(F.col("population") > 0) & 
		(F.col("ViolentCrimesPerPop") > 0)
	)

	print(df_filtered.stat.corr("medFamInc","ViolentCrimesPerPop"))

def pergunta12(df):

	print("Pergunta 12")
	
	df_filtered = df_new.select(
		"county", "community", "communityname", "population", "ViolentCrimesPerPop", 
					"racepctblack", "racePctWhite", "racePctAsian", "racePctHisp").where(
		(F.col("county") > 0) & 
		(F.col("community") > 0) & 
		(F.col("population") > 0) & 
		(F.col("racepctblack") > 0) & 
		(F.col("racePctWhite") > 0) & 
		(F.col("racePctAsian") > 0) & 
		(F.col("racePctHisp") > 0) & 
		(F.col("ViolentCrimesPerPop") > 0)
	)
	df_10_communities = df_filtered.orderBy("ViolentCrimesPerPop","population", ascending=False).limit(10)

	df_10_communities.groupBy().agg(
		{"racepctblack":"sum", "racePctWhite":"sum", "racePctAsian":"sum", "racePctHisp":"sum"}).show()

if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Communities & Crime]"))

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          #.schema(schema_communities_crime)
		          .load("./data/communities-crime/communities-crime.csv"))
	# print(df.show())
	df_new = treatcsv(df)
	# pergunta1(df_new)
	# pergunta2(df_new)
	# pergunta3(df_new)
	# pergunta4(df_new)
	# pergunta5(df_new)
	# pergunta6(df_new)
	# pergunta7(df_new)
	# pergunta8(df_new)
	# pergunta9(df_new)
	# pergunta10(df_new)
	# pergunta11(df_new)
	pergunta12(df_new)

