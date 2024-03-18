-- Databricks notebook source
-- MAGIC %run /utils/spark_utils

-- COMMAND ----------

-- MAGIC %py
-- MAGIC load_parquet_to_temp_table("/mnt/prd/fprp_notification/lc/blast/blast_list_overall","blast_list_overall")

-- COMMAND ----------

create
or replace temp view blast_list as
select
  distinct vip_no,
  blast_date,
  case
    when cust_type = "Hong Kong" then "HK"
    when cust_type = "China" then "CN"
    when cust_type = "Overseas" then "ROW"
  end as blast_region,
  case
    when promotion_edm_flg = "Y"
    and email is not null then "EDM"
    else "SMS"
  end as blast_channel
from
  blast_list_overall;
select
  *
from
  blast_list

-- COMMAND ----------

-- MAGIC %py
-- MAGIC campaign_name_keyword= "fprp_notification"
-- MAGIC campaign_category="Always On - FPRP"
-- MAGIC
-- MAGIC def get_individual_campaign_revenue(blast_list_table_name):
-- MAGIC     df=spark.sql(f"""
-- MAGIC         select 
-- MAGIC         distinct 
-- MAGIC         B.blast_date,
-- MAGIC         B.blast_region,
-- MAGIC         B.blast_channel,
-- MAGIC         B.vip_no,
-- MAGIC         sum(A.amt_hkd) as campaign_revenue_hkd,
-- MAGIC         sum(A.dtl_qty) as campaign_purchase_qty,
-- MAGIC         count(distinct A.order_no) campaign_transaction
-- MAGIC         from lc_prd.crm_db_neo_silver.dbo_v_sales_dtl A
-- MAGIC         right join lc_prd.crm_db_neo_silver.dbo_v_fprp_redeem_by_quarter C on A.order_no=C.order_no 
-- MAGIC         right join {blast_list_table_name} B on A.vip_no = B.vip_no and to_date(A.order_date,"yyyyMMdd")>=B.blast_date and  to_date(A.order_date,"yyyyMMdd")<= DateADD(B.blast_date,7)
-- MAGIC         group by 1,2,3,4
-- MAGIC         having sum(A.dtl_qty)>0
-- MAGIC         """)
-- MAGIC     df.createOrReplaceTempView("individual_campaign_revenue")
-- MAGIC     
-- MAGIC def get_individual_email_response():
-- MAGIC     df=spark.sql(f"""
-- MAGIC         select 
-- MAGIC         distinct 
-- MAGIC         DL.id as campaign_id ,
-- MAGIC         DL.name as campaign_name,
-- MAGIC         DL.date_sent  as blast_date,
-- MAGIC         split_part(DL.name,"_" ,5) as blast_region,
-- MAGIC         CT.vip_no,
-- MAGIC         case when CT.vip_no is not null then 1 else 0 end  as send,
-- MAGIC         case when source = 'mo' then 1 else 0 end  as open,
-- MAGIC         case when source = 'click' then 1 else 0 end  as click
-- MAGIC         from lc_prd.api_emarsys_silver.campaign_details_combined DL
-- MAGIC         inner join lc_prd.api_emarsys_silver.campaign_contact_combined CT on DL.id = CT.campaign_id
-- MAGIC         inner join lc_prd.api_emarsys_silver.campaign_response_combined RS on RS.id=DL.id and RS.vip_no = CT.vip_no
-- MAGIC         where lower(name) like "%{campaign_name_keyword}%"
-- MAGIC         and campaign_category="{campaign_category}"
-- MAGIC         and CT.vip_no not like "%test%"
-- MAGIC         """)
-- MAGIC     df.createOrReplaceTempView("individual_email_response")
-- MAGIC
-- MAGIC get_individual_campaign_revenue(blast_list_table_name="blast_list")
-- MAGIC get_individual_email_response()
-- MAGIC

-- COMMAND ----------

create
or replace temp view monthly_kpi as
select
  "FPRP Notification" as campaign_name,
  Year(C.blast_date) as blast_year,
  lpad(month(C.blast_date), 2) as blast_month,
  C.blast_region,
  C.blast_channel,
  count(distinct C.vip_no) as campaign_size,
  sum(B.open) as edm_open,
  sum(B.click) as edm_click,
  count(distinct A.vip_no) as number_of_purchaser,
  SUM(campaign_transaction) as number_of_transaction,
  sum(A.campaign_revenue_hkd) as campaign_revenue,
  ROUND(sum(A.campaign_purchase_qty), 0) as campaign_quantity,
  count(distinct A.vip_no) / count(distinct C.vip_no) as conversion_rate
from
  blast_list C
  left join individual_campaign_revenue A using (vip_no, blast_date)
  left join individual_email_response B on A.vip_no = B.vip_no
  and A.blast_date = B.blast_date
  and A.blast_region = B.blast_region
  and A.blast_channel = "EDM"
group by
  1,
  2,
  3,
  4,
  5;
select
  *
from
  monthly_kpi

-- COMMAND ----------

-- MAGIC %py
-- MAGIC import os 
-- MAGIC base_dir= "/dbfs/mnt/prd/fprp_notification/lc/monitor"
-- MAGIC os.makedirs(base_dir,exist_ok=True)
-- MAGIC write_temp_table_to_parquet("monthly_kpi","/mnt/prd/fprp_notification/lc/monitor/fprp_notification_monthly_kpi")
