from pyspark.sql import functions as F
from delta.tables import DeltaTable

def get_changes_df():
    silver_payment = DeltaTable.forName(spark, "nyc_taxi.silver_payment")
    latest_version = silver_payment.history().select("version").first()[0]
    df_changes = spark.sql(f"""
    SELECT payment_type_id, payment_type
    FROM table_changes('nyc_taxi.silver_payment', {latest_version})
    """)
    return df_changes.dropDuplicates(["payment_type_id"])


def merge_scd_1(df_changes):
    dim_payment = DeltaTable.forName(spark, "nyc_taxi.dim_payment")
    dim_payment.alias("target")\
                .merge(df_changes.alias("source"), "target.payment_type_id = source.payment_type_id")\
                .whenMatchedUpdate(
                    set = {
                    "payment_type_id": "source.payment_type_id",
                    "payment_type": "source.payment_type"
                })\
                .whenNotMatchedInsert(
                    values = {
                        "payment_type_id": "source.payment_type_id",
                        "payment_type": "source.payment_type"
                    }
                )\
                .execute()
    
merge_scd_1(get_changes_df())