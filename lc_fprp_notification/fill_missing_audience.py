# Databricks notebook source
# MAGIC %run "/utils/spark_utils"

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("campaign_id", "")
dbutils.widgets.text("group", "")
dbutils.widgets.text("region", "")

campaign_id = getArgument("campaign_id")
group = getArgument("group")
region = getArgument("region")

# COMMAND ----------

# fill in missing audience
import pyspark.sql.functions as f
from pyspark.sql.window import Window

w = Window().orderBy(f.lit(1))

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

audience_path = f"/Volumes/lc_prd/ml_cdxp_p13n_silver/audience/{campaign_id}_{group}/audience_list"
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
