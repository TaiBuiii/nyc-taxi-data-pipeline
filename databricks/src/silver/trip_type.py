from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import DataFrame


def get_changes_df() -> DataFrame:

    # Load bronze_green_trip Delta Table instance
    bronze_type = DeltaTable.forName(spark,"nyc_taxi.bronze_type")

    # Get the latest version
    latest_version = bronze_type.history(1).select("version").first()[0]

    # Get the newly inserted DataFrame from bronze green trip
    changes_df = spark.sql(f"""
        SELECT *
        FROM table_changes(
            'nyc_taxi.bronze_type',
            {latest_version}
        )
        WHERE _change_type = 'insert'
    """)
    return changes_df


def process_trip_type(df:DataFrame):
    df = df.select(
        F.col("trip_type").cast("int").alias("trip_type_id"),
        F.trim(F.col("description").cast("string")).alias("trip_type"),
        F.col("created_on")
    )

    df = df.withColumn("modified_on", F.current_timestamp())

    return df

def merge_trip_type(df_final:DataFrame):
    silver_type = DeltaTable.forName(spark, "nyc_taxi.silver_type")
    silver_type.alias("target")\
                .merge(df_final.alias("source"), "target.trip_type_id = source.trip_type_id")\
                .whenNotMatchedInsertAll()\
                .execute()

def main():
    df = get_changes_df()
    df_final = process_trip_type(df)
    merge_trip_type(df_final)

main()
