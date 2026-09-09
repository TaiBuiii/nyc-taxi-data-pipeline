from pyspark.sql import functions as F
from delta.tables import DeltaTable

def get_changes_df():
    silver_zone = DeltaTable.forName(spark, "nyc_taxi.silver_zone")
    latest_version = silver_zone.history().select("version").first()[0]
    df_changes = spark.sql(f"""
                    SELECT 
                        zone_id,
                        borough,
                        service_zone,
                        zone1,
                        zone2
                    FROM table_changes("nyc_taxi.silver_zone", 1)
                """)
    return df_changes.dropDuplicates(["zone_id"])

def process_dim_zone(df):
    dim_zone = DeltaTable.forName(spark, "nyc_taxi.dim_zone")

    dim_zone.alias("target").merge(
        df.alias("source"),
        "target.zone_id = source.zone_id"
    ).whenMatchedUpdate(
        set = {
            "borough": "source.borough",
            "service_zone": "source.service_zone",
            "zone1": "source.zone1",
            "zone2": "source.zone2"
        }
    ).whenNotMatchedInsert(
        values = {
            "zone_id": "source.zone_id",
            "borough": "source.borough",
            "service_zone": "source.service_zone",
            "zone1": "source.zone1",
            "zone2": "source.zone2"
        }
    ).execute()

process_dim_zone(get_changes_df())