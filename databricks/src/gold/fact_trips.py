from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
import sys 

def get_changes_df():
    silver_trip = DeltaTable.forName(spark, "nyc_taxi.silver_green_trip")
    latest_version = silver_trip.history().select("version").first()[0]
    df_changes = spark.sql(f"""
            SELECT 
                deterministic_hash_key,
                pickup_datetime,
                dropoff_datetime,
                pickup_zone_id,
                dropoff_zone_id,
                payment_id,
                trip_type_id,
                passenger_count,
                trip_distance,
                fare_amount,
                total_amount,
                year,
                month,
                trip_duration,
                average_speed,
                extra_charge
            FROM table_changes("nyc_taxi.silver_green_trip", {latest_version})
    """)
    return df_changes.dropDuplicates(["deterministic_hash_key"])

def transform_fact_trips(silver_trip : DataFrame):
    
    # Read dimesion tables in gold layer
    dim_zone = spark.read.table("nyc_taxi.dim_zone")
    dim_type = spark.read.table("nyc_taxi.dim_type")
    dim_payment = spark.read.table("nyc_taxi.dim_payment")
        
    # Join silver data with dimension tables
    fact_trip = silver_trip\
            .join(dim_zone.alias("pu"), silver_trip.pickup_zone_id == F.col("pu.zone_id"), "left")\
            .join(dim_zone.alias("do"), silver_trip.dropoff_zone_id == F.col("do.zone_id"), "left")\
            .join(dim_payment, silver_trip.payment_id == dim_payment.payment_type_id, "left")\
            .join(dim_type, silver_trip.trip_type_id == dim_type.trip_type_id, "left")\
            .select(
                F.col("deterministic_hash_key").alias("trip_id"),
                F.col("pu.zone_sk").alias("pickup_zone_sk"),
                F.col("do.zone_sk").alias("dropoff_zone_sk"),
                dim_type.trip_type_sk,
                dim_payment.payment_type_sk,
                "pickup_datetime",
                "dropoff_datetime",
                "passenger_count",
                "trip_distance",
                "fare_amount",
                "total_amount",
                "extra_charge",
                "trip_duration",
                "average_speed",
                "year",
                "month"
            )
    return fact_trip
        
def mergeToFactTrips(df : DataFrame):
    fact_trips = DeltaTable.forName(spark, "nyc_taxi.fact_trips")
    fact_trips.alias("target").merge(df.alias("source"), "target.trip_id = source.trip_id")\
        .whenMatchedUpdate(
            set = {
                "pickup_zone_sk" : "source.pickup_zone_sk",
                "dropoff_zone_sk" : "source.dropoff_zone_sk",
                "trip_type_sk" : "source.trip_type_sk",
                "payment_type_sk" : "source.payment_type_sk",
                "pickup_datetime" : "source.pickup_datetime",
                "dropoff_datetime" : "source.dropoff_datetime",
                "passenger_count" : "source.passenger_count",
                "trip_distance" : "source.trip_distance",
                "fare_amount" : "source.fare_amount",
                "total_amount" : "source.total_amount",
                "extra_charge" : "source.extra_charge",
                "trip_duration" : "source.trip_duration",
                "average_speed" : "source.average_speed",
                "year" : "source.year",
                "month" : "source.month"
            }
        )\
        .whenNotMatchedInsert(
            values = {
                "trip_id" : "source.trip_id",
                "pickup_zone_sk" : "source.pickup_zone_sk",
                "dropoff_zone_sk" : "source.dropoff_zone_sk",
                "trip_type_sk" : "source.trip_type_sk",
                "payment_type_sk" : "source.payment_type_sk",
                "pickup_datetime" : "source.pickup_datetime",
                "dropoff_datetime" : "source.dropoff_datetime",
                "passenger_count" : "source.passenger_count",
                "trip_distance" : "source.trip_distance",
                "fare_amount" : "source.fare_amount",
                "total_amount" : "source.total_amount",
                "extra_charge" : "source.extra_charge",
                "trip_duration" : "source.trip_duration",
                "average_speed" : "source.average_speed",
                "year" : "source.year",
                "month" : "source.month"            
            }
        )\
        .execute()

def process_fact_trips():
    df = get_changes_df()
    df_transformed = transform_fact_trips(df)
    mergeToFactTrips(df_transformed)
    
process_fact_trips()
