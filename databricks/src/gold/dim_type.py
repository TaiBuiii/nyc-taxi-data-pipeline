from pyspark.sql import functions as F
from delta.tables import DeltaTable

def get_changes_df():
    silver_type = DeltaTable.forName(spark, "nyc_taxi.silver.type")
    latest_version = silver_type.history().select("version").first()[0]
    df_changes = spark.sql(f"""
    SELECT trip_type_id, trip_type 
    FROM table_changes('nyc_taxi.silver.type', {latest_version})
    """)
    return df_changes.dropDuplicates(["trip_type_id"])


def merge_scd_1(df_changes):
    dim_type = DeltaTable.forName(spark, "nyc_taxi.gold.dim_type")
    dim_type.alias("target")\
                .merge(df_changes.alias("source"), "target.trip_type_id = source.trip_type_id")\
                .whenMatchedUpdate(
                    set = {
                    "trip_type_id": "source.trip_type_id",
                    "trip_type": "source.trip_type"
                })\
                .whenNotMatchedInsert(
                    values = {
                        "trip_type_id": "source.trip_type_id",
                        "trip_type": "source.trip_type"
                    }
                )\
                .execute()
    
merge_scd_1(get_changes_df())