from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="portfolio_taxi_gold_daily",
    comment="Daily taxi business KPIs",
    table_properties={
        "quality": "gold"
    }
)
def taxi_gold_daily():

    df = spark.read.table(
        "portfolio_taxi_silver"
    )

    return (
        df
        .groupBy("pickup_date")

        .agg(

            F.count("*")
            .alias("total_trips"),

            F.round(
                F.sum("fare_amount"),
                2
            ).alias("total_revenue"),

            F.round(
                F.avg("fare_amount"),
                2
            ).alias("avg_fare"),

            F.round(
                F.avg("trip_distance"),
                2
            ).alias("avg_trip_distance"),

            F.round(
                F.avg("trip_duration_minutes"),
                2
            ).alias("avg_trip_duration_minutes"),

            F.round(
                F.avg("fare_per_mile"),
                2
            ).alias("avg_fare_per_mile")
        )
    )