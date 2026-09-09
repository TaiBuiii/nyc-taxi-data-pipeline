from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import DataFrame


def get_changes_df() -> DataFrame:

    # Load bronze_green_trip Delta Table instance
    bronze_payment = DeltaTable.forName(spark,"nyc_taxi.bronze_payment")

    # Get the latest version
    latest_version = bronze_payment.history(1).select("version").first()[0]

    # Get the newly inserted DataFrame from bronze green trip
    changes_df = spark.sql(f"""
        SELECT *
        FROM table_changes(
            'nyc_taxi.bronze_payment',
            {latest_version}
        )
        WHERE _change_type = 'insert'
    """)
    return changes_df


def process_trip_payment(df:DataFrame):
    df = df.select(
        F.col("payment_type_code").cast("int").alias("payment_type_id"),
        F.trim(F.col("payment_type").cast("string")).alias("payment_type"),
        F.col("created_on")
    )

    df = df.withColumn("modified_on", F.current_timestamp())
    return df

def merge_trip_payment(df_final:DataFrame):
    silver_payment = DeltaTable.forName(spark, "nyc_taxi.silver_payment")
    silver_payment.alias("target")\
                .merge(df_final.alias("source"), "target.payment_type_id = source.payment_type_id")\
                .whenNotMatchedInsertAll()\
                .execute()
def main():
    df = get_changes_df()
    df_final = process_trip_payment(df)
    merge_trip_payment(df_final)

main()

