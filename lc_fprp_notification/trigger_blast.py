# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.dropdown("test", "false", ["true", "false"])

# COMMAND ----------

# Get audience_id & campaign_id from customization_id
def get_customization(customization_id):
    rows = spark.sql(
        "SELECT audience_id, campaign_id "
        "FROM lc_prd.ml_cdxp_p13n_silver.customization_config "
        f"WHERE customization_id = '{customization_id}'"
    ).collect()
    if not rows:
        raise ValueError(f"customization_id '{customization_id}' not found")
    campaign_id = rows[0].campaign_id
    audience_ids = [row.audience_id for row in rows]
    return campaign_id, audience_ids

# COMMAND ----------

regions = ["hk", "cn"]
groups = ["female", "male"]

# COMMAND ----------

for region in regions:
    campaign_id, audience_ids = get_customization(f"lc_fprp_notification_{region}")
    blast_date = campaign_id.split("_")[-2]
    for group in groups:
        # fill in missing audience
        dbutils.notebook.run(
            "./fill_missing_audience", 
            0,
            {
                "campaign_id": campaign_id,
                "group": group,
                "region": region,
            }
        )


# COMMAND ----------

dbutils.notebook.run(
    "./prepare_blast",
    0,
    {
        "blast_date": blast_date,
        "test": getArgument("test"),
    },
)
