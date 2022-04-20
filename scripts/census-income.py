from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.window import Window

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

    df_new = isNA(df, 'age')
    df_new = isNA(df_new, 'workclass')
    df_new = isNA(df_new, 'education')
    df_new = isNA(df_new, 'education-num')
    df_new = isNA(df_new, 'marital-status')
    df_new = isNA(df_new, 'occupation')
    df_new = isNA(df_new, 'relashionship')
    df_new = isNA(df_new, 'race')
    df_new = isNA(df_new, 'sex')
    df_new = isNA(df_new, 'capital-gain')
    df_new = isNA(df_new, 'capital-loss')
    df_new = isNA(df_new, 'hours-per-week')
    df_new = isNA(df_new, 'native-country')
    df_new = isNA(df_new, 'income')
    
    df_new = typeChange(df_new, "education-num", "int")
    df_new = typeChange(df_new, "capital-gain", "int")
    df_new = typeChange(df_new, "capital-loss", "int")
    df_new = typeChange(df_new, "hours-per-week", "int")
    
    return df_new

def pergunta1(df):

	print("Pergunta 1")

	df_filter = df.select("workclass").where((F.col("income").rlike(">50K")) & (~F.col("workclass").contains("?")))
	df_filter.groupBy("workclass").count().orderBy("count", ascending=False).show()

def pergunta2(df):

	print("Pergunta 2")

	df_filter = df.select("race", "hours-per-week").where((~F.col("race").contains("?")) & (~F.col("hours-per-week").contains("?")))
	df_filter.groupBy("race").avg("hours-per-week").orderBy("avg(hours-per-week)",ascending=False).show()

def pergunta3(df):

	print("Pergunta 3")

	df_filter = df.select("sex").where(~F.col("sex").contains("?"))
	df_grouped = df_filter.groupBy("sex").count()
	df_grouped = df_grouped.withColumn(
		"percent",
		F.col("count")/F.sum("count").over(Window.partitionBy())
	)
	df_grouped.select("*").where(F.col("sex").contains("Male")).show()

def pergunta4(df):

	print("Pergunta 4")
	df_filter = df.select("sex").where(~F.col("sex").contains("?"))
	df_grouped = df_filter.groupBy("sex").count()
	df_grouped = df_grouped.withColumn(
		"percent",
		F.col("count")/F.sum("count").over(Window.partitionBy())
	)
	df_grouped.select("*").where(F.col("sex").contains("Female")).show()

def pergunta5(df):

	print("Pergunta 5")

	df_filter = (df.select("workclass", "hours-per-week")
				.where(
					(~F.col("workclass").contains("?")) & 
					(~F.col("hours-per-week").contains("?"))
					))
	df_filter.groupBy("workclass").avg("hours-per-week").orderBy("avg(hours-per-week)",ascending=False).show(1)

def pergunta6(df):

	print("Pergunta 6")

	df_filter = (df.select("workclass", "education", "education-num")
             .where(
                 ~(F.col("workclass").contains("?")) &
                 ~(F.col("education").contains("?")) &
                 ~(F.col("education-num").contains("?"))
             ))
	df_filter.groupBy("education-num", "education", "workclass").count().distinct().orderBy("count",ascending=False).show(1)

def pergunta7(df):

	print("Pergunta 7")

	df_filter = (df.select("workclass", "sex")
             .where(
                 ~(F.col("workclass").contains("?")) &
                 ~(F.col("sex").contains("?"))
             ))
	(df_filter.groupBy("sex", "workclass").count().distinct().orderBy("count",ascending=False)
	.where(F.col("sex").contains("Male")).show(1))

	(df_filter.groupBy("sex", "workclass").count().distinct().orderBy("count",ascending=False)
	.where(F.col("sex").contains("Female")).show(1))

def pergunta8(df):

	print("Pergunta 8")

	df_filter = (df.select("race", "education", "education-num")
             .where(
                 ~(F.col("race").contains("?")) &
                 ~(F.col("education").contains("?")) &
                 ~(F.col("education-num").contains("?"))
             ))
	(df_filter.groupBy("race", "education-num", "education").count().distinct().orderBy("count",ascending=False)
	.where(F.col("race").contains("White")).show(1))
	(df_filter.groupBy("race", "education-num", "education").count().distinct().orderBy("count",ascending=False)
	.where(F.col("race").contains("Black")).show(1))
	(df_filter.groupBy("race", "education-num", "education").count().distinct().orderBy("count",ascending=False)
	.where(F.col("race").contains("Asian-Pac-Islander")).show(1))
	(df_filter.groupBy("race", "education-num", "education").count().distinct().orderBy("count",ascending=False)
	.where(F.col("race").contains("Amer-Indian-Eskimo")).show(1))
	(df_filter.groupBy("race", "education-num", "education").count().distinct().orderBy("count",ascending=False)
	.where(F.col("race").contains("Other")).show(1))

def pergunta9(df):

	print("Pergunta 9")

	df_filter = (df.select("education-num", "education", "sex", "race", "workclass")
             .where(
                 ~(F.col("education-num").contains("?")) &
                 ~(F.col("education").contains("?")) &
                 ~(F.col("sex").contains("?")) &
                 ~(F.col("race").contains("?")) &
                 ~(F.col("workclass").contains("?")) &
                 (F.col("workclass").rlike("Self-emp-inc"))
             ))
	(df_filter.groupBy("education-num", "education", "sex", "race", "workclass").count().distinct()
	.orderBy("count",ascending=False).show())


def pergunta10(df):

	print("Pergunta 10")
	df_filter = df.select("marital-status").where(~F.col("marital-status").contains("?"))
	df_filter_new = df_filter.withColumn(
		"marital-status",
		F.when(F.col("marital-status").rlike("Married"), "Married").otherwise("Not Married"))
	df_grouped = df_filter_new.groupBy("marital-status").count()
	df_grouped = df_grouped.withColumn(
		"percent",
		F.col("count")/F.sum("count").over(Window.partitionBy())
	)
	df_grouped.show()


def pergunta11(df):

	print("Pergunta 11")

	df_filter = (df.select("race", "marital-status")
             .where(
                 (~F.col("race").contains("?")) & 
                 (~F.col("marital-status").contains("?"))
             ))
	df_filter_new = df_filter.withColumn(
		"marital-status",
		F.when(F.col("marital-status").rlike("Married"), "Married").otherwise("Not Married"))
	(df_filter_new.groupBy("race", "marital-status").count().distinct()
	.orderBy("count",ascending=False).where(F.col("marital-status").contains("Not Married")).show(1))


def pergunta12(df):

	print("Pergunta 12")

	df_filter = (df.select("marital-status", "income")
             .where(
                 (~F.col("income").contains("?")) & 
                 (~F.col("marital-status").contains("?"))
             ))
	df_filter_new = df_filter.withColumn(
		"marital-status",
		F.when(F.col("marital-status").rlike("Married"), "Married").otherwise("Not Married"))

	(df_filter_new.groupBy("marital-status", "income").count().distinct()
	.orderBy("count",ascending=False).where(F.col("marital-status").startswith("Not")).show())

	(df_filter_new.groupBy("marital-status", "income").count().distinct()
	.orderBy("count",ascending=False).where(~F.col("marital-status").startswith("Not")).show())


def pergunta13(df):

	print("Pergunta 13")

	df_filter = (df.select("sex", "income")
             .where(
                 (~F.col("income").contains("?")) & 
                 (~F.col("sex").contains("?"))
             ))
	(df_filter.groupBy("sex", "income").count().distinct()
	.orderBy("count", ascending=False).where(F.col("sex").contains("Male")).show(1))

	(df_filter.groupBy("sex", "income").count().distinct()
	.orderBy("count", ascending=False).where(F.col("sex").contains("Female")).show(1))


def pergunta14(df):

	print("Pergunta 14")

	df_filter = (df.select("income", "native-country")
             .where(
                 (~F.col("income").contains("?")) & 
                 (~F.col("native-country").contains("?"))
             ))
	df_country = df_filter.select("native-country").distinct()
	countrys = df_country.toPandas()
	for index, row in countrys.iterrows():
		
		(df_filter.groupBy("native-country", "income").count().distinct()
		.orderBy("count", ascending=False).where(F.col("native-country").contains(row[0])).show(1))


def pergunta15(df):

	print("Pergunta 15")

	df_filter = (df.select("race")
             .where(
                 (~F.col("race").contains("?"))
             ))
	df_filter_new = df_filter.withColumn(
		"race",
		F.when(F.col("race").rlike("White"), "White").otherwise("Not White"))

	df_grouped = df_filter_new.groupBy("race").count()
	df_grouped = df_grouped.withColumn(
		"percent",
		F.col("count")/F.sum("count").over(Window.partitionBy())
	)
	df_grouped.show()


if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Census Income]"))

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          #.schema(schema_census_income)
		          .load("/home/spark/capgemini-aceleracao-pyspark/data/census-income/census-income.csv"))
	#print(df.show())
	df_new = treatcsv(df)
	pergunta1(df_new)
	pergunta2(df_new)
	pergunta3(df_new)
	pergunta4(df_new)
	pergunta5(df_new)
	pergunta6(df_new)
	pergunta7(df_new)
	pergunta8(df_new)
	pergunta9(df_new)
	pergunta10(df_new)
	pergunta11(df_new)
	pergunta12(df_new)
	pergunta13(df_new)
	pergunta14(df_new)
	pergunta15(df_new)