from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import DataFrame


def get_changes_df() -> DataFrame:

    # Load bronze_green_trip Delta Table instance
    bronze_zone = DeltaTable.forName(spark,"nyc_taxi.bronze_zone")

    # Get the latest version
    latest_version = bronze_zone.history(1).select("version").first()[0]

    # Get the newly inserted DataFrame from bronze green trip
    changes_df = spark.sql(f"""
        SELECT *
        FROM table_changes(
            'nyc_taxi.bronze_zone',
            {latest_version}
        )
        WHERE _change_type = 'insert'
    """)
    return changes_df


def process_trip_zone(df:DataFrame):
    df = df.select(
            F.col("LocationID").cast("int").alias("zone_id"),
            F.trim(F.col("Borough").cast("string")).alias("borough"),
            F.trim(F.col("Zone").cast("string")).alias("zone"),
            F.trim(F.col("service_zone").cast("string")).alias("service_zone"),
            F.col("created_on")
    )

    df = df.withColumn("zone1", F.get(F.split(F.col("zone"), "/"), 0))\
            .withColumn("zone2", F.get(F.split(F.col("zone"), "/"), 1))\
            .drop("zone")


    df = df.withColumn("modified_on", F.current_timestamp())

    return df

def merge_trip_zone(df_final:DataFrame):
    silver_zone = DeltaTable.forName(spark, "nyc_taxi.silver_zone")
    silver_zone.alias("target")\
                .merge(df_final.alias("source"), "target.zone_id = source.zone_id")\
                .whenNotMatchedInsertAll()\
                .execute()

def main():
    df = get_changes_df()
    df_final = process_trip_zone(df)
    merge_trip_zone(df_final)

main()

