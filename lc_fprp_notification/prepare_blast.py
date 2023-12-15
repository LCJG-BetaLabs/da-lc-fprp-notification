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
# MAGIC OR REPLACE TEMPORARY VIEW recommendations_non_beauty_hk AS
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   Recommendations_female_hk
# MAGIC union all
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   Recommendations_male_hk;
# MAGIC
# MAGIC
# MAGIC CREATE
# MAGIC OR REPLACE TEMPORARY VIEW recommendations_non_beauty_cn AS
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   Recommendations_male_cn
# MAGIC   union all
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   Recommendations_female_cn;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TEMPORARY VIEW BlastList_NON_BEAUTY_hk AS
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
# MAGIC   recommendations_non_beauty_hk r
# MAGIC   INNER JOIN customer_list_hk m USING (vip_no)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TEMPORARY VIEW BlastList_NON_BEAUTY_cn AS
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
# MAGIC   recommendations_non_beauty_cn r
# MAGIC   INNER JOIN customer_list_cn m USING (vip_no)

# COMMAND ----------

blast_list_nb_hk = spark.table("BlastList_NON_BEAUTY_hk").toPandas()
blast_list_nb_cn = spark.table("BlastList_NON_BEAUTY_cn").toPandas()

# COMMAND ----------

def get_utm_source(group):
    if group.startswith("Female"):
        return "female_DB"
    return "Male_DB"

# COMMAND ----------

blast_list_nb_hk["utm_source"] = blast_list_nb_hk["group"].apply(get_utm_source)
blast_list_nb_hk["utm_campaign"] = f"gg_prodlisting-fprp_notification_{blast_date}"

blast_list_nb_cn["utm_source"] = blast_list_nb_cn["group"].apply(get_utm_source)
blast_list_nb_cn["utm_campaign"] = f"gg_prodlisting-fprp_notification_{blast_date}"

# COMMAND ----------

# check for missing / duplicate atg codes
import numpy as np

atg = blast_list_nb_hk["atg_code"].str.split("|").tolist()
nuniques = [len(set([a for a in l if len(a) == 6])) for l in atg if l != None] # len(a) == 12 for valid atg code
RAISE_ERROR = False
if list(set(nuniques)) != [8]:
    problematic_idx = np.where(np.array(nuniques) != 8)[0]
    print(blast_list_nb_hk.iloc[problematic_idx][["vip_no", "atg_code"]])
    if (not RAISE_ERROR) or len(problematic_idx) <= 8:
        print("WARNING: Missing or duplicate atg code in these rows. Removing these rows.")
        blast_list_nb_hk = blast_list_nb_hk.drop(index=problematic_idx)
    else:
        raise Exception("Missing or duplicate atg code in these rows!")

atg = blast_list_nb_cn["atg_code"].str.split("|").tolist()
nuniques = [len(set([a for a in l if len(a) == 6])) for l in atg if l != None] # len(a) == 12 for valid atg code
RAISE_ERROR = False
if list(set(nuniques)) != [8]:
    problematic_idx = np.where(np.array(nuniques) != 8)[0]
    print(blast_list_nb_cn.iloc[problematic_idx][["vip_no", "atg_code"]])
    if (not RAISE_ERROR) or len(problematic_idx) <= 8:
        print("WARNING: Missing or duplicate atg code in these rows. Removing these rows.")
        blast_list_nb_cn = blast_list_nb_cn.drop(index=problematic_idx)
    else:
        raise Exception("Missing or duplicate atg code in these rows!")

# COMMAND ----------

# REMEMBER TO DEDUP FIRST 4 BRANDS AND ADD THE 4TH BRAND
# brand_nb=np.unique(blast_list_nb["brand"].str.split("|").values.flatten())
brand = blast_list_nb_hk["brand"].str.split("|").tolist()
# brand_nb=np.unique(brand)
blast_list_nb_hk["top_1_brand"] = [",".join(list(set(a))[:1]) for a in brand]
blast_list_nb_hk["top_2_brand"] = [",".join(list(set(a))[1:2]) for a in brand]
blast_list_nb_hk["top_3_brand"] = [",".join(list(set(a))[2:3]) for a in brand]

# brand_nb=np.unique(blast_list_nb["brand"].str.split("|").values.flatten())
brand = blast_list_nb_cn["brand"].str.split("|").tolist()
# brand_nb=np.unique(brand)
blast_list_nb_cn["top_1_brand"] = [",".join(list(set(a))[:1]) for a in brand]
blast_list_nb_cn["top_2_brand"] = [",".join(list(set(a))[1:2]) for a in brand]
blast_list_nb_cn["top_3_brand"] = [",".join(list(set(a))[2:3]) for a in brand]

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


emarsys_list_1_hk = blast_list_nb_hk.copy().reset_index(drop=True)
emarsys_list_1_cn = blast_list_nb_cn.copy().reset_index(drop=True)

# COMMAND ----------

# MAGIC %py
# MAGIC df = spark.createDataFrame(emarsys_list_1_hk)
# MAGIC df.createOrReplaceTempView("hk_list")
# MAGIC
# MAGIC df = spark.createDataFrame(emarsys_list_1_cn)
# MAGIC df.createOrReplaceTempView("cn_list")

# COMMAND ----------

def product_recommendation(region):
    spark.sql(f"""
CREATE
OR REPLACE TEMPORARY VIEW product_recommendation_{region} AS
select
  campaign_id,
  customer_id,
  product_id,
  display_id,
  created_date,
  blast_date
from
  (
    select
      campaign_id,
      vip_no as customer_id,
      atg_code_1 as product_id,
      1 as display_id,
      current_date() as created_date,
      blast_date
    from
      {region}_list
    union ALL
    select
      campaign_id,
      vip_no as customer_id,
      atg_code_2 as product_id,
      2 as display_id,
      current_date() as created_date,
      blast_date
    from
      {region}_list
    union ALL
    select
      campaign_id,
      vip_no as customer_id,
      atg_code_3 as product_id,
      3 as display_id,
      current_date() as created_date,
      blast_date
    from
      {region}_list
    union ALL
    select
      campaign_id,
      vip_no as customer_id,
      atg_code_4 as product_id,
      4 as display_id,
      current_date() as created_date,
      blast_date
    from
      {region}_list
    union ALL
    select
      campaign_id,
      vip_no as customer_id,
      atg_code_5 as product_id,
      5 as display_id,
      current_date() as created_date,
      blast_date
    from
      {region}_list
    union ALL
    select
      campaign_id,
      vip_no as customer_id,
      atg_code_6 as product_id,
      6 as display_id,
      current_date() as created_date,
      blast_date
    from
      {region}_list
    union ALL
    select
      campaign_id,
      vip_no as customer_id,
      atg_code_7 as product_id,
      7 as display_id,
      current_date() as created_date,
      blast_date
    from
      {region}_list
    union ALL
    select
      campaign_id,
      vip_no as customer_id,
      atg_code_8 as product_id,
      8 as display_id,
      current_date() as created_date,
      blast_date
    from
      {region}_list
  )
order by
  customer_id,
  display_id
  """)

product_recommendation(region="hk")
product_recommendation(region="cn")

# COMMAND ----------

def products(region):
    spark.sql(f"""
CREATE
OR REPLACE TEMPORARY VIEW products_{region} AS
select
  distinct product_id,
  brand_desc_en,
  product_description_en,
  global_exclusive,
  coalesce(product_description_cn,product_description_en) as product_description_cn,
  brand_description_cn,
  created_date
from(
    select
      distinct atg_code_1 as product_id,
      brand_desc_1 as brand_desc_en,
      prod_desc_eng as product_description_en,
      Global_Exclusive_1 as global_exclusive,
      '' as image_url,
      prod_desc_tc as product_description_cn,
      brand_desc as brand_description_cn,
      current_date() as created_date
    from
      {region}_list a
      inner join lc_prd.ml_data_preproc_silver.aggregated_item_master b on a.atg_code_1 = b.atg_code
    union all
    select
      distinct atg_code_2 as product_id,
      brand_desc_2 as brand_desc_en,
      prod_desc_eng as product_description_en,
      Global_Exclusive_2 as global_exclusive,
      '' as image_url,
      prod_desc_tc as product_description_cn,
      brand_desc as brand_description_cn,
      current_date() as created_date
    from
      {region}_list a
      left join lc_prd.ml_data_preproc_silver.aggregated_item_master b on a.atg_code_2 = b.atg_code
    union all
    select
      distinct atg_code_3 as product_id,
      brand_desc_3 as brand_desc_en,
      prod_desc_eng as product_description_en,
      Global_Exclusive_3 as global_exclusive,
      '' as image_url,
      prod_desc_tc as product_description_cn,
      brand_desc as brand_description_cn,
      current_date() as created_date
    from
      {region}_list a
      left join lc_prd.ml_data_preproc_silver.aggregated_item_master b on a.atg_code_3 = b.atg_code
    union all
    select
      distinct atg_code_4 as product_id,
      brand_desc_4 as brand_desc_en,
      prod_desc_eng as product_description_en,
      Global_Exclusive_4 as global_exclusive,
      '' as image_url,
      prod_desc_tc as product_description_cn,
      brand_desc as brand_description_cn,
      current_date() as created_date
    from
      {region}_list a
      left join lc_prd.ml_data_preproc_silver.aggregated_item_master b on a.atg_code_4 = b.atg_code
    union all
    select
      distinct atg_code_5 as product_id,
      brand_desc_5 as brand_desc_en,
      prod_desc_eng as product_description_en,
      Global_Exclusive_5 as global_exclusive,
      '' as image_url,
      prod_desc_tc as product_description_cn,
      brand_desc as brand_description_cn,
      current_date() as created_date
    from
      {region}_list a
      left join lc_prd.ml_data_preproc_silver.aggregated_item_master b on a.atg_code_5 = b.atg_code
    union all
    select
      distinct atg_code_6 as product_id,
      brand_desc_6 as brand_desc_en,
      prod_desc_eng as product_description_en,
      Global_Exclusive_6 as global_exclusive,
      '' as image_url,
      prod_desc_tc as product_description_cn,
      brand_desc as brand_description_cn,
      current_date() as created_date
    from
      {region}_list a
      left join lc_prd.ml_data_preproc_silver.aggregated_item_master b on a.atg_code_6 = b.atg_code
    union all
    select
      distinct atg_code_7 as product_id,
      brand_desc_7 as brand_desc_en,
      prod_desc_eng as product_description_en,
      Global_Exclusive_7 as global_exclusive,
      '' as image_url,
      prod_desc_tc as product_description_cn,
      brand_desc as brand_description_cn,
      current_date() as created_date
    from
      {region}_list a
      left join lc_prd.ml_data_preproc_silver.aggregated_item_master b on a.atg_code_7 = b.atg_code
    union all
    select
      distinct atg_code_8 as product_id,
      brand_desc_8 as brand_desc_en,
      prod_desc_eng as product_description_en,
      Global_Exclusive_8 as global_exclusive,
      '' as image_url,
      prod_desc_tc as product_description_cn,
      brand_desc as brand_description_cn,
      current_date() as created_date
    from
      {region}_list a
      left join lc_prd.ml_data_preproc_silver.aggregated_item_master b on a.atg_code_8 = b.atg_code
  )
              """)

product(region= "hk")
product(region= "cn")

# COMMAND ----------

def campaign_personalization(region):
    spark.sql(f"""
CREATE
OR REPLACE TEMPORARY VIEW campaign_personalization_{region} as
select
  distinct campaign_id,
  vip_no as customer_id,
  blast_date,
  customer_name_shortened_chi as customer_name,
  '' as subject_line,
  '' as message,
  Language,
  '' as title,
  'email' as utm_medium,
  utm_source,
  utm_campaign,
  current_date() as created_date
from
  {region}_list
              """)

campaign_personalization(region="hk")
campaign_personalization(region="cn")

# COMMAND ----------

import pandas as pd
import os

base_dir = "/dbfs/mnt/prd/fprp_notification/lc/blast"
os.makedirs(base_dir, exist_ok=True)

def save_output (region):
    campaign_personalization = spark.table(f"campaign_personalization_{region}").toPandas()
    campaign_personalization.to_csv(
        os.path.join(base_dir, f"campaign_personalization_fprp_notification_%s_{region}.upsert.csv" % (blast_date)),
        index=False,
        encoding="utf-8",
    )
    products = spark.table(f"products_{region}").toPandas()
    products.to_csv(
        os.path.join(base_dir, f"products_fprp_notification_%s_{region}.upsert.csv" % (blast_date)),
        index=False,
        encoding="utf-8",
    )
    product_recommendation = spark.table(f"product_recommendation_{region}").toPandas()
    product_recommendation.to_csv(
        os.path.join(base_dir, f"product_recommendation_fprp_notification_%s_{region}.upsert.csv" % (blast_date)),
        index=False,
        encoding="utf-8",
    )
save_output (region="hk")
save_output (region="cn")

# COMMAND ----------


def sendgrid_df(region):
    spark.sql(f"""
              CREATE OR REPLACE TEMPORARY VIEW sendgrid_df_{region} AS
WITH eligible_audience AS (
  SELECT 
    "Eligible List" AS source,
    COUNT(DISTINCT vip_no) AS vips,
    " "
  FROM customer_list_{region}
),
rec AS (
  SELECT
    "Recommendation List" AS source,
    COUNT(DISTINCT vip_no) AS vips,
    " "
  FROM recommendations_non_beauty_{region}
),
blast_count AS (
  SELECT
    "Blast" AS source,
    COUNT(DISTINCT customer_id) AS vips,
    blast_date
  FROM campaign_personalization_{region}
  GROUP BY blast_date
)
SELECT * FROM eligible_audience
UNION
SELECT * FROM rec
UNION ALL
SELECT "", "", ""
UNION ALL
SELECT " ", "blast_size", "blast_date"
UNION
SELECT * FROM blast_count

              """)
sendgrid_df(region="hk")
sendgrid_df(region="cn")

# COMMAND ----------

from datetime import date
import pandas as pd

today = date.today()
html_table1=spark.table("sendgrid_df_hk").toPandas().to_html()
html_table2=spark.table("sendgrid_df_cn").toPandas().to_html()
email_body = f"<h2>Hong Kong</h2>{html_table1}<h2>Mainland</h2>{html_table2}"
email_subject = f"LC FPRP Notification monitor report ({today})"
send_email(["arnabmaulik@lcjgroup.com", "cintiaching@lcjgroup.com"], email_subject, email_body)

# COMMAND ----------

if test:
    spark.table("hk_list").write.parquet(os.path.join(base_dir.replace("/dbfs", ""), "blast_list_hk"), mode="append")
