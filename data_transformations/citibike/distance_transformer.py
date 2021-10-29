from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql import functions as F

METERS_PER_FOOT = 0.3048
FEET_PER_MILE = 5280
EARTH_RADIUS_IN_METERS = 6371e3
METERS_PER_MILE = METERS_PER_FOOT * FEET_PER_MILE


def compute_distance(_spark: SparkSession, dataframe: DataFrame) -> DataFrame:
    df_reduce = dataframe.select('bikeid', 'start_station_latitude', 'end_station_latitude', 'start_station_longitude',
                          'end_station_longitude')
    df_reduce.show()

    dataframe = df_reduce.withColumn("haversine", (
            F.pow(F.sin(F.radians(F.col("end_station_latitude") - F.col("start_station_latitude")) / 2), 2) +
            F.cos(F.radians(F.col("start_station_latitude"))) * F.cos(F.radians(F.col("end_station_latitude"))) *
            F.pow(F.sin(F.radians(F.col("end_station_longitude") - F.col("start_station_longitude")) / 2), 2)
    )).withColumn("distance",
                  (F.atan2(F.sqrt(F.col("haversine")), F.sqrt(-F.col("haversine") + 1)) * 12742000) * 0.621371)
    dataframe.show(5)

    return dataframe


def run(spark: SparkSession, input_dataset_path: str, transformed_dataset_path: str) -> None:
    input_dataset = spark.read.parquet(input_dataset_path)
    input_dataset.show()

    dataset_with_distances = compute_distance(spark, input_dataset)
    dataset_with_distances.show()

    dataset_with_distances.write.format('parquet').save(transformed_dataset_path, mode='overwrite')