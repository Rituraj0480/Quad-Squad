from pyspark.sql import SparkSession, functions, types
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

@functions.udf(returnType=types.StringType())
def pass_types(x):
  if x=='Pay Per Ride' or x=='24 Hour' or x=='30 Day Pass':
    return 'Short Term'
  elif x=='365 Day Pass Standard' or x=='365 Day Pass Plus':
    return 'Long Term'
  elif x=='365 Corporate Standard' or x=='365 Corporate Plus':
    return 'Corporate'
  else:
    return 'Others'

@functions.udf(returnType=types.StringType())
def date_func(x):
  try:
    return x.split(' ')[0]
  except:
    return x


@functions.udf(returnType=types.StringType())
def season_func(x):
  month = int(x.split('-')[1])
  if month>=3 and month<=5:
    return 'Spring'
  elif month>=6 and month<=8:
    return 'Summer'
  elif month>=9 and month<=11:
    return 'Fall'
  else:
    return 'Winter'


def main(input, map, weather, output):
    schema = types.StructType([
    types.StructField('Departure1', types.StringType()),
    types.StructField('Return1', types.StringType()),
    types.StructField('Electric Bike', types.BooleanType()),
    types.StructField('Departure station', types.StringType()),
    types.StructField('Return station', types.StringType()),
    types.StructField('Membership type', types.StringType()),
    types.StructField('Covered distance (m)', types.FloatType()),
    types.StructField('Duration (s)', types.IntegerType()),
    types.StructField('Departure temperature (C)', types.IntegerType()),
    types.StructField('Return temperature (C)', types.IntegerType()),
    types.StructField('Stopover duration (s)', types.IntegerType()),
    types.StructField('Number of stopovers', types.IntegerType()),
])

    map_schema = types.StructType([
    types.StructField('Departure station', types.StringType()),
    types.StructField('Latitude', types.FloatType()),
    types.StructField('Longitude', types.FloatType()),
])

    df = spark.read.csv(input, schema=schema)
    df = df.filter(df['Covered distance (m)'].isNotNull())
    df = df.withColumn("Pass Type", pass_types(df['Membership type'])).withColumn("Departure", date_func(df['Departure1'])).withColumn("Return", date_func(df['Return1']))
    df = df.drop('Departure1', 'Return1', 'Electric bike', 'Electric Bike', 'Bike')
    df = df.withColumn('Seasons', season_func(df['Departure']))

    map_df = spark.read.csv(map, schema=map_schema)

    df_final = df.alias('a').join(map_df.alias('b'), 'Departure station', 'leftouter').select('a.*', functions.col('b.Latitude').alias('dep_latitude'), functions.col('b.Longitude').alias('dep_longitude'))

    df_final2 = df_final.alias('a').join(map_df.alias('b'), [df_final['Return station']==map_df['Departure station']], 'leftouter').select('a.*', functions.col('b.Latitude').alias('ret_latitude'), functions.col('b.Longitude').alias('ret_longitude'))


    weather_df = spark.read.csv(weather, header=True, inferSchema=True)
    weather_df = weather_df.withColumnRenamed('date', 'Departure')
    final_df_t = df_final2.join(weather_df, 'Departure', 'leftouter')
    final_df = final_df_t.drop("max_dew_point_v", "max_dew_point_s", 'max_dew_point_c', 'max_dew_point_d', 'max_relative_humidity_v', 'max_relative_humidity_s', 'max_relative_humidity_c', 'max_relative_humidity_d', 'max_temperature_c', 'max_temperature_d', 'max_wind_speed_s', 'max_wind_speed_c', 'max_wind_speed_d','min_dew_point_v', 'min_dew_point_s', 'min_dew_point_c','min_dew_point_d','min_relative_humidity_v', 'min_relative_humidity_s', 'min_relative_humidity_c', 'min_relative_humidity_d', 'min_temperature_s', 'min_temperature_c', 'min_temperature_d','min_wind_speed_s','min_wind_speed_c','min_wind_speed_d','precipitation_s','precipitation_c','precipitation_d', 'rain_s','rain_c', 'rain_d', 'snow_s', 'snow_c', 'snow_d', 'snow_on_ground_s', 'snow_on_ground_c', 'snow_on_ground_d', 'solar_radiation_v', 'solar_radiation_s', 'solar_radiation_c', 'solar_radiation_d')
    
    final_df.write.option("header", True).csv(output, mode='overwrite')
    #final_df.show(10)

    


if __name__ == '__main__':
    input = sys.argv[1]
    map = sys.argv[2]
    weather = sys.argv[3]
    output = sys.argv[4]
    spark = SparkSession.builder.appName('code').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input, map, weather, output)


