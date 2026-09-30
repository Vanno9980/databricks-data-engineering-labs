from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="portfolio_taxi_bronze",
    comment="Raw NYC taxi trips",
    table_properties={
        "quality": "bronze"
    }
)
def taxi_bronze():

    return (
        spark.readStream
        .table("samples.nyctaxi.trips")
        .withColumn(
            "ingestion_timestamp",
            F.current_timestamp()
        )
    )