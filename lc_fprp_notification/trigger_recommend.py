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
from typing import List


def add_days_to_date(date, days):
    added_date = pd.to_datetime(date) + timedelta(days=days)
    added_date = added_date.strftime("%Y%m%d")
    return added_date


today = datetime.datetime.today()
blast_date = add_days_to_date(today, 1)
today = today.strftime("%Y%m%d")
today, blast_date

# COMMAND ----------

def set_customization(customization_id: str, campaign_id: str, audience_ids: List[str]):
    # remove old ids
    spark.sql(f"""
        DELETE FROM lc_prd.ml_cdxp_p13n_silver.customization_config
        WHERE customization_id = "{customization_id}";
    """)
    # insert new ids
    for audience_id in audience_ids:
        spark.sql(f"""
            INSERT INTO lc_prd.ml_cdxp_p13n_silver.customization_config
            SELECT 
                "{customization_id}" AS customization_id, 
                "{audience_id}" AS audience_id, 
                "{campaign_id}" AS campaign_id;
        """)

# COMMAND ----------

groups = ["male", "female"]
regions = ["hk", "cn"]
blast_date_dash = datetime.datetime.strftime(datetime.datetime.strptime(blast_date, "%Y%m%d"), "%Y-%m-%d")
cdp_base_dir = "/Volumes/lc_prd/ml_cdxp_p13n_silver/"

# COMMAND ----------

for region in regions:
    print(f"region = {region}")
    dbutils.notebook.run(
        "./generate_audience_list_and_item_list", 
        0,
        {
            "blast_date": blast_date,
            "region": region
        }
    )
    
    campaign_id = f"lc_fprp_notification_{blast_date}_{region}"
    campaign_dir = os.path.join(cdp_base_dir, "campaign", campaign_id)
    os.makedirs(campaign_dir, exist_ok=True)
    print("campaign_dir:", campaign_dir)

    audience_ids = []
    audience_base_dir = os.path.join(cdp_base_dir, "audience")
    print("audience_base_dir:", audience_base_dir)

    for group in groups:
        audience_id = f"lc_fprp_notification_{blast_date}_{region}_{group}"
        audience_dir = os.path.join(audience_base_dir, audience_id)
        print("audience_dir:", audience_dir)
        dbutils.notebook.run("./generate_audience_config", 0, {"audience_id": audience_id, "region": region})
        audience_ids.append(audience_id)

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
    set_customization(
        customization_id=f"lc_fprp_notification_{region}",
        campaign_id=campaign_id,
        audience_ids=audience_ids,
    )

# COMMAND ----------


