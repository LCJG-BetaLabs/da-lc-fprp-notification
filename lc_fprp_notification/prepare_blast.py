# Databricks notebook source
# MAGIC %run "/utils/sendgrid_utils"

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("blast_date", "")
dbutils.widgets.dropdown("test", "false", ["true", "false"])

blast_date = getArgument("blast_date")
test = getArgument("test") == "true"
regions = ["hk", "cn"]

# COMMAND ----------

import os

cdp_base_dir = "/Volumes/lc_prd/ml_cdxp_p13n_silver/"

for region in regions:
    spark.read.parquet(
        os.path.join(cdp_base_dir, "audience", f"lc_fprp_notification_{region}.parquet")
    ).createOrReplaceTempView(f"customer_list_{region}")

    spark.table(
        f"lc_prd.ml_cdxp_p13n_silver.campaign_lc_fprp_notification_{blast_date}_{region}_version_male_final_output"
    ).createOrReplaceTempView(f"Recommendations_male_{region}")
    spark.table(
        f"lc_prd.ml_cdxp_p13n_silver.campaign_lc_fprp_notification_{blast_date}_{region}_version_female_final_output"
    ).createOrReplaceTempView(f"Recommendations_female_{region}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TEMPORARY VIEW recommendations_non_beauty AS
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   Recommendations_female_hk
# MAGIC union all
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   Recommendations_male_hk
# MAGIC union all
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   Recommendations_male_cn
# MAGIC   union all
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   Recommendations_female_cn

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TEMPORARY VIEW customer_list_all AS
# MAGIC select * from customer_list_hk
# MAGIC union 
# MAGIC select * from customer_list_cn

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TEMPORARY VIEW BlastList_NON_BEAUTY AS
# MAGIC SELECT
# MAGIC   r.vip_no,
# MAGIC   atg_code_1,
# MAGIC   atg_code_2,
# MAGIC   atg_code_3,
# MAGIC   atg_code_4,
# MAGIC   atg_code_5,
# MAGIC   atg_code_6,
# MAGIC    atg_code_7,
# MAGIC   atg_code_8,
# MAGIC   brand_desc_1,
# MAGIC   brand_desc_2,
# MAGIC   brand_desc_3,
# MAGIC   brand_desc_4,
# MAGIC   brand_desc_5,
# MAGIC   brand_desc_6,
# MAGIC   brand_desc_7,
# MAGIC   brand_desc_8,
# MAGIC   prod_desc_1,
# MAGIC   prod_desc_2,
# MAGIC   prod_desc_3,
# MAGIC   prod_desc_4,
# MAGIC   prod_desc_5,
# MAGIC   prod_desc_6,
# MAGIC   prod_desc_7,
# MAGIC   prod_desc_8,
# MAGIC   Global_Exclusive_1,
# MAGIC   Global_Exclusive_2,
# MAGIC   Global_Exclusive_3,
# MAGIC   Global_Exclusive_4,
# MAGIC   Global_Exclusive_5,
# MAGIC   Global_Exclusive_6,
# MAGIC   Global_Exclusive_7,
# MAGIC   Global_Exclusive_8,
# MAGIC   concat(
# MAGIC     brand_desc_1,
# MAGIC     "|",
# MAGIC     brand_desc_2,
# MAGIC     "|",
# MAGIC     brand_desc_3,
# MAGIC     "|",
# MAGIC     brand_desc_4,
# MAGIC     "|",
# MAGIC       brand_desc_5,
# MAGIC     "|",
# MAGIC     brand_desc_6,
# MAGIC     "|",
# MAGIC     brand_desc_7,
# MAGIC     "|",
# MAGIC     brand_desc_8
# MAGIC   ) as brand,
# MAGIC   concat(
# MAGIC     atg_code_1,
# MAGIC     "|",
# MAGIC     atg_code_2,
# MAGIC     "|",
# MAGIC     atg_code_3,
# MAGIC     "|",
# MAGIC       atg_code_4,
# MAGIC     "|",
# MAGIC     atg_code_5,
# MAGIC     "|",
# MAGIC     atg_code_6,
# MAGIC     "|",
# MAGIC     atg_code_7,
# MAGIC     "|",
# MAGIC     atg_code_8
# MAGIC   ) as atg_code,
# MAGIC   DATE_ADD(current_date(), 1) as blast_date,
# MAGIC   m.promotion_edm_flg,
# MAGIC   m.email,
# MAGIC   m.card_type,
# MAGIC group,
# MAGIC   m.customer_name_shortened_chi,
# MAGIC   pref_lang AS Language,
# MAGIC   cust_type,
# MAGIC   'fprp_notification' as campaign_id 
# MAGIC FROM
# MAGIC   recommendations_non_beauty r
# MAGIC   INNER JOIN customer_list_all m USING (vip_no)

# COMMAND ----------

blast_list_nb = spark.table("BlastList_NON_BEAUTY").toPandas()

# COMMAND ----------

def get_utm_source(group):
    if group.startswith("Female"):
        return "female_DB"
    return "Male_DB"

# COMMAND ----------

blast_list_nb["utm_source"] = blast_list_nb["group"].apply(get_utm_source)
blast_list_nb["utm_campaign"] = f"gg_prodlisting-fprp_notification_{blast_date}"

# COMMAND ----------

# check for missing / duplicate atg codes
import numpy as np

atg = blast_list_nb["atg_code"].str.split("|").tolist()
nuniques = [len(set([a for a in l if len(a) == 6])) for l in atg if l != None] # len(a) == 12 for valid atg code
RAISE_ERROR = False
if list(set(nuniques)) != [8]:
    problematic_idx = np.where(np.array(nuniques) != 8)[0]
    print(blast_list_nb.iloc[problematic_idx][["vip_no", "atg_code"]])
    if (not RAISE_ERROR) or len(problematic_idx) <= 8:
        print("WARNING: Missing or duplicate atg code in these rows. Removing these rows.")
        blast_list_nb = blast_list_nb.drop(index=problematic_idx)
    else:
        raise Exception("Missing or duplicate atg code in these rows!")

# COMMAND ----------

# REMEMBER TO DEDUP FIRST 4 BRANDS AND ADD THE 4TH BRAND
# brand_nb=np.unique(blast_list_nb["brand"].str.split("|").values.flatten())
brand = blast_list_nb["brand"].str.split("|").tolist()
# brand_nb=np.unique(brand)
blast_list_nb["top_1_brand"] = [",".join(list(set(a))[:1]) for a in brand]
blast_list_nb["top_2_brand"] = [",".join(list(set(a))[1:2]) for a in brand]
blast_list_nb["top_3_brand"] = [",".join(list(set(a))[2:3]) for a in brand]

# COMMAND ----------

emarsys_columns_nb = [
    "vip_no",
    "blast_date",
    "atg_code_1",
    "atg_code_2",
    "atg_code_3",
    "atg_code_4",
    "atg_code_5",
    "atg_code_6",
    "atg_code_7",
    "atg_code_8",
    "brand_desc_1",
    "brand_desc_2",
    "brand_desc_3",
    "brand_desc_4",
    "brand_desc_5",
    "brand_desc_6",
    "brand_desc_7",
    "brand_desc_8",
    "prod_desc_1",
    "prod_desc_2",
    "prod_desc_3",
    "prod_desc_4",
    "prod_desc_5",
    "prod_desc_6",
    "prod_desc_7",
    "prod_desc_8",
    "Global_Exclusive_1",
    "Global_Exclusive_2",
    "Global_Exclusive_3",
    "Global_Exclusive_4",
    "Global_Exclusive_5",
    "Global_Exclusive_6",
    "Global_Exclusive_7",
    "Global_Exclusive_8",
    "utm_source",
    "utm_campaign",
    "card_type",
    "top_1_brand",
    "top_2_brand",
    "top_3_brand",
    "Language",
    "campaign_id" "customer_name_shortened_chi",
    "group",
]


emarsys_list_1 = blast_list_nb.copy().reset_index(drop=True)

# COMMAND ----------

# MAGIC %py
# MAGIC df = spark.createDataFrame(emarsys_list_1)
# MAGIC df.createOrReplaceTempView("All_list")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TEMPORARY VIEW product_recommendation AS
# MAGIC select
# MAGIC   campaign_id,
# MAGIC   customer_id,
# MAGIC   product_id,
# MAGIC   display_id,
# MAGIC   created_date,
# MAGIC   blast_date
# MAGIC from
# MAGIC   (
# MAGIC     select
# MAGIC       campaign_id,
# MAGIC       vip_no as customer_id,
# MAGIC       atg_code_1 as product_id,
# MAGIC       1 as display_id,
# MAGIC       current_date() as created_date,
# MAGIC       blast_date
# MAGIC     from
# MAGIC       All_list
# MAGIC     union ALL
# MAGIC     select
# MAGIC       campaign_id,
# MAGIC       vip_no as customer_id,
# MAGIC       atg_code_2 as product_id,
# MAGIC       2 as display_id,
# MAGIC       current_date() as created_date,
# MAGIC       blast_date
# MAGIC     from
# MAGIC       all_list
# MAGIC     union ALL
# MAGIC     select
# MAGIC       campaign_id,
# MAGIC       vip_no as customer_id,
# MAGIC       atg_code_3 as product_id,
# MAGIC       3 as display_id,
# MAGIC       current_date() as created_date,
# MAGIC       blast_date
# MAGIC     from
# MAGIC       all_list
# MAGIC     union ALL
# MAGIC     select
# MAGIC       campaign_id,
# MAGIC       vip_no as customer_id,
# MAGIC       atg_code_4 as product_id,
# MAGIC       4 as display_id,
# MAGIC       current_date() as created_date,
# MAGIC       blast_date
# MAGIC     from
# MAGIC       all_list
# MAGIC     union ALL
# MAGIC     select
# MAGIC       campaign_id,
# MAGIC       vip_no as customer_id,
# MAGIC       atg_code_5 as product_id,
# MAGIC       5 as display_id,
# MAGIC       current_date() as created_date,
# MAGIC       blast_date
# MAGIC     from
# MAGIC       all_list
# MAGIC     union ALL
# MAGIC     select
# MAGIC       campaign_id,
# MAGIC       vip_no as customer_id,
# MAGIC       atg_code_6 as product_id,
# MAGIC       6 as display_id,
# MAGIC       current_date() as created_date,
# MAGIC       blast_date
# MAGIC     from
# MAGIC       all_list
# MAGIC     union ALL
# MAGIC     select
# MAGIC       campaign_id,
# MAGIC       vip_no as customer_id,
# MAGIC       atg_code_7 as product_id,
# MAGIC       7 as display_id,
# MAGIC       current_date() as created_date,
# MAGIC       blast_date
# MAGIC     from
# MAGIC       all_list
# MAGIC     union ALL
# MAGIC     select
# MAGIC       campaign_id,
# MAGIC       vip_no as customer_id,
# MAGIC       atg_code_8 as product_id,
# MAGIC       8 as display_id,
# MAGIC       current_date() as created_date,
# MAGIC       blast_date
# MAGIC     from
# MAGIC       all_list
# MAGIC   )
# MAGIC order by
# MAGIC   customer_id,
# MAGIC   display_id

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TEMPORARY VIEW products AS
# MAGIC select
# MAGIC   distinct product_id,
# MAGIC   brand_desc_en,
# MAGIC   product_description_en,
# MAGIC   global_exclusive,
# MAGIC   coalesce(product_description_cn,product_description_en) as product_description_cn,
# MAGIC   brand_description_cn,
# MAGIC   created_date
# MAGIC from(
# MAGIC     select
# MAGIC       distinct atg_code_1 as product_id,
# MAGIC       brand_desc_1 as brand_desc_en,
# MAGIC       prod_desc_eng as product_description_en,
# MAGIC       Global_Exclusive_1 as global_exclusive,
# MAGIC       '' as image_url,
# MAGIC       prod_desc_tc as product_description_cn,
# MAGIC       brand_desc as brand_description_cn,
# MAGIC       current_date() as created_date
# MAGIC     from
# MAGIC       All_list a
# MAGIC       inner join lc_prd.ml_data_preproc_silver.aggregated_item_master b on a.atg_code_1 = b.atg_code
# MAGIC     union all
# MAGIC     select
# MAGIC       distinct atg_code_2 as product_id,
# MAGIC       brand_desc_2 as brand_desc_en,
# MAGIC       prod_desc_eng as product_description_en,
# MAGIC       Global_Exclusive_2 as global_exclusive,
# MAGIC       '' as image_url,
# MAGIC       prod_desc_tc as product_description_cn,
# MAGIC       brand_desc as brand_description_cn,
# MAGIC       current_date() as created_date
# MAGIC     from
# MAGIC       All_list a
# MAGIC       left join lc_prd.ml_data_preproc_silver.aggregated_item_master b on a.atg_code_2 = b.atg_code
# MAGIC     union all
# MAGIC     select
# MAGIC       distinct atg_code_3 as product_id,
# MAGIC       brand_desc_3 as brand_desc_en,
# MAGIC       prod_desc_eng as product_description_en,
# MAGIC       Global_Exclusive_3 as global_exclusive,
# MAGIC       '' as image_url,
# MAGIC       prod_desc_tc as product_description_cn,
# MAGIC       brand_desc as brand_description_cn,
# MAGIC       current_date() as created_date
# MAGIC     from
# MAGIC       All_list a
# MAGIC       left join lc_prd.ml_data_preproc_silver.aggregated_item_master b on a.atg_code_3 = b.atg_code
# MAGIC     union all
# MAGIC     select
# MAGIC       distinct atg_code_4 as product_id,
# MAGIC       brand_desc_4 as brand_desc_en,
# MAGIC       prod_desc_eng as product_description_en,
# MAGIC       Global_Exclusive_4 as global_exclusive,
# MAGIC       '' as image_url,
# MAGIC       prod_desc_tc as product_description_cn,
# MAGIC       brand_desc as brand_description_cn,
# MAGIC       current_date() as created_date
# MAGIC     from
# MAGIC       All_list a
# MAGIC       left join lc_prd.ml_data_preproc_silver.aggregated_item_master b on a.atg_code_4 = b.atg_code
# MAGIC     union all
# MAGIC     select
# MAGIC       distinct atg_code_5 as product_id,
# MAGIC       brand_desc_5 as brand_desc_en,
# MAGIC       prod_desc_eng as product_description_en,
# MAGIC       Global_Exclusive_5 as global_exclusive,
# MAGIC       '' as image_url,
# MAGIC       prod_desc_tc as product_description_cn,
# MAGIC       brand_desc as brand_description_cn,
# MAGIC       current_date() as created_date
# MAGIC     from
# MAGIC       All_list a
# MAGIC       left join lc_prd.ml_data_preproc_silver.aggregated_item_master b on a.atg_code_5 = b.atg_code
# MAGIC     union all
# MAGIC     select
# MAGIC       distinct atg_code_6 as product_id,
# MAGIC       brand_desc_6 as brand_desc_en,
# MAGIC       prod_desc_eng as product_description_en,
# MAGIC       Global_Exclusive_6 as global_exclusive,
# MAGIC       '' as image_url,
# MAGIC       prod_desc_tc as product_description_cn,
# MAGIC       brand_desc as brand_description_cn,
# MAGIC       current_date() as created_date
# MAGIC     from
# MAGIC       All_list a
# MAGIC       left join lc_prd.ml_data_preproc_silver.aggregated_item_master b on a.atg_code_6 = b.atg_code
# MAGIC     union all
# MAGIC     select
# MAGIC       distinct atg_code_7 as product_id,
# MAGIC       brand_desc_7 as brand_desc_en,
# MAGIC       prod_desc_eng as product_description_en,
# MAGIC       Global_Exclusive_7 as global_exclusive,
# MAGIC       '' as image_url,
# MAGIC       prod_desc_tc as product_description_cn,
# MAGIC       brand_desc as brand_description_cn,
# MAGIC       current_date() as created_date
# MAGIC     from
# MAGIC       All_list a
# MAGIC       left join lc_prd.ml_data_preproc_silver.aggregated_item_master b on a.atg_code_7 = b.atg_code
# MAGIC     union all
# MAGIC     select
# MAGIC       distinct atg_code_8 as product_id,
# MAGIC       brand_desc_8 as brand_desc_en,
# MAGIC       prod_desc_eng as product_description_en,
# MAGIC       Global_Exclusive_8 as global_exclusive,
# MAGIC       '' as image_url,
# MAGIC       prod_desc_tc as product_description_cn,
# MAGIC       brand_desc as brand_description_cn,
# MAGIC       current_date() as created_date
# MAGIC     from
# MAGIC       All_list a
# MAGIC       left join lc_prd.ml_data_preproc_silver.aggregated_item_master b on a.atg_code_8 = b.atg_code
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TEMPORARY VIEW campaign_personalization as
# MAGIC select
# MAGIC   distinct campaign_id,
# MAGIC   vip_no as customer_id,
# MAGIC   blast_date,
# MAGIC   customer_name_shortened_chi as customer_name,
# MAGIC   '' as subject_line,
# MAGIC   '' as message,
# MAGIC   Language,
# MAGIC   '' as title,
# MAGIC   'email' as utm_medium,
# MAGIC   utm_source,
# MAGIC   utm_campaign,
# MAGIC   current_date() as created_date
# MAGIC from
# MAGIC   All_list

# COMMAND ----------



# COMMAND ----------

import pandas as pd
import os

base_dir = "/dbfs/mnt/prd/fprp_notification/lc/blast"
os.makedirs(base_dir, exist_ok=True)
campaign_personalization = spark.table("campaign_personalization").toPandas()
campaign_personalization.to_csv(
    os.path.join(base_dir, "campaign_personalization_fprp_notification_%s.upsert.csv" % (blast_date)),
    index=False,
    encoding="utf-8",
)
products = spark.table("products").toPandas()
products.to_csv(
    os.path.join(base_dir, "products_fprp_notification_%s.upsert.csv" % (blast_date)),
    index=False,
    encoding="utf-8",
)
product_recommendation = spark.table("product_recommendation").toPandas()
product_recommendation.to_csv(
    os.path.join(base_dir, "product_recommendation_fprp_notification_%s.upsert.csv" % (blast_date)),
    index=False,
    encoding="utf-8",
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW sendgrid_df AS
# MAGIC WITH eligible_audience AS (
# MAGIC   SELECT 
# MAGIC     "Eligible List" AS source,
# MAGIC     COUNT(DISTINCT vip_no) AS vips,
# MAGIC     " "
# MAGIC   FROM customer_list_all
# MAGIC ),
# MAGIC rec AS (
# MAGIC   SELECT
# MAGIC     "Recommendation List" AS source,
# MAGIC     COUNT(DISTINCT vip_no) AS vips,
# MAGIC     " "
# MAGIC   FROM recommendations_non_beauty
# MAGIC ),
# MAGIC blast_count AS (
# MAGIC   SELECT
# MAGIC     "Blast" AS source,
# MAGIC     COUNT(DISTINCT customer_id) AS vips,
# MAGIC     blast_date
# MAGIC   FROM campaign_personalization
# MAGIC   GROUP BY blast_date
# MAGIC )
# MAGIC SELECT * FROM eligible_audience
# MAGIC UNION
# MAGIC SELECT * FROM rec
# MAGIC UNION ALL
# MAGIC SELECT "", "", ""
# MAGIC UNION ALL
# MAGIC SELECT " ", "blast_size", "blast_date"
# MAGIC UNION
# MAGIC SELECT * FROM blast_count

# COMMAND ----------

from datetime import date
import pandas as pd

today = date.today()
email_body = spark.table("sendgrid_df").toPandas().to_html()
email_subject = f"LC FPRP Notification monitor report ({today})"
send_email(["arnabmaulik@lcjgroup.com", "cintiaching@lcjgroup.com"], email_subject, email_body)

# COMMAND ----------

if test:
    spark.table("All_list").write.parquet(os.path.join(base_dir.replace("/dbfs", ""), "blast_list_overall"), mode="append")
