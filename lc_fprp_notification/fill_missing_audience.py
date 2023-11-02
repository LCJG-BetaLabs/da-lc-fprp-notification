# Databricks notebook source
# MAGIC %run "/utils/spark_utils"

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.window import Window


def fill_missing_audience(campaign_id, group, region):
    w = Window().orderBy(f.lit(1))
    output = spark.table(
        f"lc_prd.ml_cdxp_p13n_silver.campaign_{campaign_id}_version_{group}_final_output"
    )
    output_count = output.select("vip_no").distinct().count()

    audience_path = f"/Volumes/lc_prd/ml_cdxp_p13n_silver/audience/{campaign_id}_{group}/audience_list"
    audience = spark.read.parquet(audience_path)
    audience_count = audience.select("vip_no").distinct().count()
    missing = audience.select("vip_no").join(output.select("vip_no"), how="left_anti", on="vip_no")
    size = missing.count()
    missing = missing.withColumn("index", f.row_number().over(w))
    extracted = output.drop("vip_no").limit(size).withColumn("index", f.row_number().over(w))
    supp = extracted.join(missing, how="inner", on="index").drop("index")

    new_output = output.union(supp.select(*list(output.columns)))
    # backup for new output (due to spark lazy evaluation)
    create_or_insertoverwrite_table(
        new_output,
        "lc_prd",
        "ml_cdxp_p13n_silver",
        f"campaign_{campaign_id}_version_{group}_final_output_backup",
        ds_managed=True,
    )
    backup = spark.table(f"lc_prd.ml_cdxp_p13n_silver.campaign_{campaign_id}_version_{group}_final_output_backup")
    new_count = backup.select("vip_no").distinct().count()
    # overwrite
    spark.sql(f"DELETE FROM lc_prd.ml_cdxp_p13n_silver.campaign_{campaign_id}_version_{group}_final_output")
    create_or_insertoverwrite_table(
        backup,
        "lc_prd",
        "ml_cdxp_p13n_silver",
        f"campaign_{campaign_id}_version_{group}_final_output",
        ds_managed=True,
    )
    # delete backup table
    spark.sql(f"""DROP TABLE IF EXISTS lc_prd.ml_cdxp_p13n_silver.campaign_{campaign_id}_version_{group}_final_output_backup""")

    print(
        f"{group}: output={output_count} audience={audience_count} missing={size} merged={new_count}"
    )

# COMMAND ----------


