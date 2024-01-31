# Databricks notebook source
# MAGIC %run "/utils/gdm_utils"

# COMMAND ----------

#dbutils.widgets.removeAll()
#dbutils.widgets.text("blast_date", "20231017")
dbutils.widgets.text("region", "hk")

blast_date = getArgument("blast_date")
region = getArgument("region").lower()

# COMMAND ----------

import os
import pandas as pd

cdp_base_dir = "/Volumes/lc_prd/ml_cdxp_p13n_silver/"
campaign_dir = os.path.join(cdp_base_dir, "campaign", f"lc_fprp_notification_{blast_date}_{region}")
os.makedirs(campaign_dir, exist_ok=True)

item_list_path = os.path.join(campaign_dir, "item_list.csv")
print(campaign_dir)

# COMMAND ----------

# MAGIC %sql
# MAGIC Create
# MAGIC or replace temporary view audience0 as
# MAGIC select
# MAGIC   distinct cast(data_as_of as date) as data_as_of,
# MAGIC   cast(a.card_start_date as date) as card_start_date,
# MAGIC   a.vip_no,
# MAGIC   a.status,
# MAGIC   a.card_type,
# MAGIC   mailing_gp,
# MAGIC   a.cust_type,
# MAGIC   gender,
# MAGIC   a.birth_mth,
# MAGIC   CASE
# MAGIC     WHEN a.country_of_residence = "CN" THEN "CN"
# MAGIC     WHEN a.country_of_residence = "HK" THEN "HK"
# MAGIC     ELSE "Others"
# MAGIC   END AS country_of_residence,
# MAGIC   promotion_edm_flg,
# MAGIC   promotion_sms_flg,
# MAGIC   a.email,
# MAGIC   a.phone_no,
# MAGIC   a.pref_lang,
# MAGIC   customer_name_shortened_chi
# MAGIC from
# MAGIC   lc_prd.crm_db_neo_silver.dbo_v_datamart_snapshot a
# MAGIC   inner join lc_prd.crm_db_neo_silver.dbo_v_member_reachable_soa b on a.vip_no = b.vip_no
# MAGIC where
# MAGIC   a.status = 'Active'
# MAGIC   and a.cust_type in ('Hong Kong', 'Overseas','China')

# COMMAND ----------

if region == "hk":
    spark.sql(
    """Create
        or replace temporary view audience as
        select
        distinct a.*,
        egc_no
        from
        audience0 a
        inner join lc_prd.crm_db_neo_silver.dbo_v_fprp_summary b on a.vip_no = b.vip_no
        where
        TO_DATE(collect_start_date, 'yyyyMMdd') >= '2024-02-01'
        and b.region in ('HKG')
    """)
elif region == "cn":
    spark.sql(
        """
        Create
        or replace temporary view audience as
        select
        distinct a.*,
        egc_no
        from
        audience0 a
        inner join lc_prd.crm_db_neo_silver.dbo_v_fprp_summary b on a.vip_no = b.vip_no
        where
        TO_DATE(collect_start_date, 'yyyyMMdd') >= '2024-02-01'
        and  b.region in ('BJG','SHG','CHD')
        """
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC create
# MAGIC or replace temporary view CUST_LIST_grouped as
# MAGIC select
# MAGIC   A.*,
# MAGIC   CASE
# MAGIC     WHEN GENDER = 'Male' then 'Male'
# MAGIC     else 'Female'
# MAGIC   end as group
# MAGIC FROM
# MAGIC   audience a

# COMMAND ----------

# MAGIC %sql
# MAGIC create
# MAGIC or replace temporary view CUST_LIST_male as
# MAGIC select
# MAGIC   distinct vip_no
# MAGIC from
# MAGIC   CUST_LIST_grouped
# MAGIC where
# MAGIC   group = 'Male';
# MAGIC create
# MAGIC   or replace temporary view CUST_LIST_female as
# MAGIC select
# MAGIC   distinct vip_no
# MAGIC from
# MAGIC   CUST_LIST_grouped
# MAGIC where
# MAGIC   group = 'Female';

# COMMAND ----------

for group in ["female", "male"]:
    audience_dir = os.path.join(cdp_base_dir, "audience", f"lc_fprp_notification_{blast_date}_{region}_{group}")
    os.makedirs(audience_dir, exist_ok=True)
    audience_list_path = os.path.join(audience_dir, "audience_list")
    print(audience_list_path)
    spark.table(f"CUST_LIST_{group}").write.parquet(audience_list_path, "overwrite")

# COMMAND ----------

# used in prepare_blast
spark.table("CUST_LIST_grouped").write.parquet(
    os.path.join(cdp_base_dir, "audience", f"lc_fprp_notification_{region}.parquet"), "overwrite"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- get new in item list in hk
# MAGIC CREATE
# MAGIC or replace temp view new_in As
# MAGIC SELECT
# MAGIC   pub.id AS atg_code,
# MAGIC   CAST(dt.start_date AS DATE) AS start_date,
# MAGIC   item.bu_desc
# MAGIC FROM
# MAGIC   lc_prd.atgclone_silver.cataloga_publication_status_lc pub
# MAGIC   LEFT JOIN lc_prd.atgclone_silver.cataloga_start_date_lc dt ON dt.id = pub.id
# MAGIC   AND dt.site_id = pub.site_id
# MAGIC   LEFT JOIN lc_prd.atgclone_silver.cataloga_dcs_prd_chldsku sku ON pub.id = sku.product_id
# MAGIC   LEFT JOIN lc_prd.atgclone_silver.atgcore_dcs_inventory inv ON inv.inventory_id = sku.sku_id || ',' || pub.site_id
# MAGIC   LEFT JOIN lc_prd.ml_data_preproc_silver.aggregated_item_master item ON item.atg_code = pub.id
# MAGIC WHERE
# MAGIC   pub.site_id = concat('zh_', upper(getArgument("region"))) --'zh_HK'
# MAGIC   AND publication_status = 2
# MAGIC   AND dt.start_date > current_date() - 20
# MAGIC   AND (
# MAGIC     class_desc <> 'Decorative Accessories'
# MAGIC     AND subclass_desc <> 'Objects'
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC Create
# MAGIC or replace temporary view new_in_list as
# MAGIC select
# MAGIC   distinct a.atg_code,
# MAGIC   b.bu_desc,
# MAGIC   start_date
# MAGIC from
# MAGIC   new_in a
# MAGIC   inner join lc_prd.ml_data_preproc_silver.aggregated_item_master b on a.atg_code = b.atg_code
# MAGIC where
# MAGIC (
# MAGIC     class_desc <> 'Decorative Accessories'
# MAGIC     and subclass_desc <> 'Objects'
# MAGIC   )
# MAGIC   and (
# MAGIC     category_desc <> 'Swimwear'
# MAGIC     and subclass_desc <> 'Briefs'
# MAGIC   )
# MAGIC   and (
# MAGIC     class_desc <> 'Fitness'
# MAGIC     and subclass_desc <> 'Fitness equipement'
# MAGIC   )
# MAGIC   and (
# MAGIC     class_desc <> 'Decorative Accessories'
# MAGIC     and subclass_desc <> 'Art & Collectibles'
# MAGIC   )
# MAGIC   and class_desc <> 'Concession Health & Wellness';
# MAGIC
# MAGIC -- select item not in hl, cos, jwy, for past 60 days
# MAGIC Create
# MAGIC   or replace temporary view new_in_list_non_hl as
# MAGIC select
# MAGIC *
# MAGIC from
# MAGIC   new_in_list
# MAGIC where
# MAGIC   bu_Desc NOT IN ('HL', 'COS', 'JWY')
# MAGIC   and start_date >= date_add(current_date(), -60);
# MAGIC
# MAGIC -- select item in hl, cos, jwy, for past 90 days
# MAGIC Create
# MAGIC   or replace temporary view new_in_list_hl as
# MAGIC   select
# MAGIC   *
# MAGIC from
# MAGIC   new_in_list
# MAGIC where
# MAGIC   bu_Desc in ('HL', 'COS', 'JWY')
# MAGIC   and start_date >= date_add(current_date(), -90);
# MAGIC
# MAGIC -- join hl and non-hl
# MAGIC Create
# MAGIC   or replace temporary view new_in_list_final as
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   new_in_list_non_hl
# MAGIC union all
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   new_in_list_hl;

# COMMAND ----------

new_in_list = spark.table("new_in_list_final").toPandas()
new_in_list.to_csv(
    item_list_path,
    index=False,
    encoding="utf-8",
)
