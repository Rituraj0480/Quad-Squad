from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('colour prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4' # make sure we have Spark 2.4+
import sys

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator


@functions.udf(returnType=types.IntegerType())
def month_f(x):
  return int(x.split('-')[1])


def main(input, output):

    data = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(input)

    data.show(5)

    df_t1 = data.groupBy('Departure', 'Pass', 'Seasons').agg(functions.count('Departure Station_x').alias('count'))

    df_t2 = df_t1.select('Departure', 'Pass', 'Seasons', 'count')
    df = df_t2.withColumn("month", month_f(df_t2['Departure']))
    train, validation = df.select('month', 'Pass', 'Seasons', 'count').randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()

    
    word_indexer1 = StringIndexer(inputCol = "Pass", outputCol="Pass_n")
    word_indexer2 = StringIndexer(inputCol = "Seasons", outputCol="Seasons_n")
    vecAssembler = VectorAssembler(inputCols=['month', 'Pass_n', 'Seasons_n'], outputCol="features")
    gbt = GBTRegressor(featuresCol="features", labelCol="count", maxIter=50)
    pipeline = Pipeline(stages=[word_indexer1, word_indexer2, vecAssembler, gbt])
    gbt_model = pipeline.fit(train)

    #create an evaluator and score the validation data
    predictions = gbt_model.transform(validation)

    r2_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='count', metricName='r2')
    r2 = r2_evaluator.evaluate(predictions)
    print('\nr2 = ',r2)

    gbt_model.write().overwrite().save(output)
    
if __name__ == '__main__':
    input = sys.argv[1]
    output = sys.argv[2]
    main(input, output)