# Databricks notebook source
# MAGIC %md
# MAGIC # Usage
# MAGIC Run `workflow` 1 day before the blast

# COMMAND ----------

# MAGIC %run "/utils/spark_utils"

# COMMAND ----------

import os
import datetime
import pandas as pd
from datetime import timedelta


def add_days_to_date(date, days):
    added_date = pd.to_datetime(date) + timedelta(days=days)
    added_date = added_date.strftime("%Y%m%d")
    return added_date


today = datetime.datetime.today()
blast_date = add_days_to_date(today, 1)
today = today.strftime("%Y%m%d")
today, blast_date

# COMMAND ----------

groups = ["male", "female"]
region = "hk"

# COMMAND ----------

dbutils.notebook.run(
    "./generate_audience_list_and_item_list", 
    0,
    {"blast_date": blast_date}
)

# COMMAND ----------

blast_date_dash = datetime.datetime.strftime(datetime.datetime.strptime(blast_date, "%Y%m%d"), "%Y-%m-%d")

cdp_base_dir = "/Volumes/lc_prd/ml_cdxp_p13n_silver/"
campaign_id = f"lc_fprp_notification_{blast_date}_{region}"
campaign_dir = os.path.join(cdp_base_dir, "campaign", campaign_id)
os.makedirs(campaign_dir, exist_ok=True)
print("campaign_dir:", campaign_dir)

# COMMAND ----------

audience_ids = []
audience_base_dir = os.path.join(cdp_base_dir, "audience")
print("audience_base_dir:", audience_base_dir)

for group in groups:
    audience_id = f"lc_fprp_notification_{blast_date}_{region}_{group}"
    audience_dir = os.path.join(audience_base_dir, audience_id)
    print("audience_dir:", audience_dir)
    dbutils.notebook.run("./generate_audience_config", 0, {"audience_id": audience_id, "region": region})
    audience_ids.append(audience_id)

# COMMAND ----------

dbutils.notebook.run(
    "./generate_campaign_config",
    0,
    {
        "campaign_id": campaign_id,
        "version_ids": ",".join(groups),
        "audience_ids": ",".join(audience_ids),
        "region": region,
    },
)

# COMMAND ----------

for audience_id in audience_ids:
    print("running audience workflow:", audience_dir)
    dbutils.notebook.run(
        "/Repos/Data Scientist/ds-cdp-core/personalization/audience/audience_workflow",
        0,
        {"audience_id": audience_id},
    )

# COMMAND ----------

dbutils.notebook.run(
    "/Repos/Data Scientist/ds-cdp-core/personalization/personalization_workflow", 
    0, 
    {
        "campaign_id": campaign_id,
        "validate_output": "False",
    },
)

# COMMAND ----------

# fill in missing audience
import pyspark.sql.functions as f
from pyspark.sql.window import Window

w = Window().orderBy(f.lit(1))

for group in groups:
    output = spark.table(
        f"lc_prd.ml_cdxp_p13n_silver.campaign_{campaign_id}_version_{group}_final_output"
    )
    output_count = output.select("vip_no").distinct().count()
    # backup
    if not spark.catalog.tableExists(
        f"lc_prd.ml_cdxp_p13n_silver.campaign_{campaign_id}_version_{group}_final_output_backup"
    ):
        create_or_insertoverwrite_table(
            output,
            "lc_prd",
            "ml_cdxp_p13n_silver",
            f"campaign_{campaign_id}_version_{group}_final_output_backup",
            ds_managed=True,
        )

    audience_path = f"/Volumes/lc_prd/ml_cdxp_p13n_silver/audience/lc_fprp_notification_{blast_date}_hk_{group}/audience_list"
    audience = spark.read.parquet(audience_path)
    audience_count = audience.select("vip_no").distinct().count()
    missing = audience.select("vip_no").join(output.select("vip_no"), how="left_anti", on="vip_no")
    size = missing.count()
    missing = missing.withColumn("index", f.row_number().over(w))
    extracted = output.drop("vip_no").limit(size).withColumn("index", f.row_number().over(w))
    supp = extracted.join(missing, how="inner", on="index").drop("index")

    x = spark.table(
        f"lc_prd.ml_cdxp_p13n_silver.campaign_{campaign_id}_version_{group}_final_output_backup"
    )
    new_output = x.union(supp.select(*list(x.columns)))
    new_count = new_output.select("vip_no").distinct().count()
    # overwrite
    spark.sql(f"DELETE FROM lc_prd.ml_cdxp_p13n_silver.campaign_{campaign_id}_version_{group}_final_output")
    create_or_insertoverwrite_table(
        new_output,
        "lc_prd",
        "ml_cdxp_p13n_silver",
        f"campaign_{campaign_id}_version_{group}_final_output",
        ds_managed=True,
    )
    
    print(
        f"{group}: output={output_count} audience={audience_count} missing={size} merged={new_count}"
    )

# COMMAND ----------

dbutils.notebook.run(
    "./prepare_blast",
    0,
    {"blast_date": blast_date},
)
