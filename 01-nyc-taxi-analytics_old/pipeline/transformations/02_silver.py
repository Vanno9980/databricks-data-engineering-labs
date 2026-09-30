from pyspark import pipelines as dp
from pyspark.sql import functions as F

silver_expectations = {

    "pickup_not_null":
        "tpep_pickup_datetime IS NOT NULL",

    "dropoff_not_null":
        "tpep_dropoff_datetime IS NOT NULL",

    "valid_trip_dates":
        "tpep_dropoff_datetime > tpep_pickup_datetime",

    "positive_distance":
        "trip_distance > 0",

    "positive_fare":
        "fare_amount > 0",

    "pickup_zip_not_null":
        "pickup_zip IS NOT NULL",

    "dropoff_zip_not_null":
        "dropoff_zip IS NOT NULL"
}

@dp.table(
    name="portfolio_taxi_silver",
    comment="Validated and enriched taxi trips",
    table_properties={
        "quality": "silver"
    }
)

@dp.expect_all_or_drop(silver_expectations)

def taxi_silver():

    df = spark.readStream.table(
        "portfolio_taxi_bronze"
    )

    return (
        df

        .withColumn(
            "trip_duration_minutes",
            (
                F.col("tpep_dropoff_datetime").cast("long")
                - F.col("tpep_pickup_datetime").cast("long")
            ) / 60
        )

        .withColumn(
            "pickup_date",
            F.to_date("tpep_pickup_datetime")
        )

        .withColumn(
            "pickup_hour",
            F.hour("tpep_pickup_datetime")
        )

        .withColumn(
            "fare_per_mile",
            F.round(
                F.col("fare_amount")
                / F.col("trip_distance"),
                2
            )
        )
    )