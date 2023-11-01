# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text("audience_id", "")
dbutils.widgets.text("region", "")

audience_id = getArgument("audience_id")
region = getArgument("region").lower()

# COMMAND ----------

import os
import time
import datetime
import json
from jinja2 import Template

cdp_base_dir = "/Volumes/lc_prd/ml_cdxp_p13n_silver"
audience_base_dir = os.path.join(cdp_base_dir, "audience")
audience_dir = os.path.join(audience_base_dir, audience_id)
audience_list_path = os.path.join(audience_dir, "audience_list")
print("audience_base_dir:", audience_base_dir)
print("audience_dir:", audience_dir)
print("audience_list_path:", audience_list_path)

# COMMAND ----------

now = time.time()

template = Template("""
{
    "filters": {
        "demography": {
            {% if region != 'row' %}
            "nationality": 
                {{ nationalities | tojson() }}
            ,
            {% endif %}
            "email_valid_flg": [
                    "Y",
                    "N"
            ]    
        },
        "loyalty": {},
        "purchase": {},
        "segment": {},
        "product": {}
    },
    "list": {
        "type": "parquet", 
        "path": "{{ audience_list_path }}"
    },
    "id": "{{ audience_id }}",
    "name": "fprp_notification",
    "version": 1,
    "updated_at": {{ now }},
    "created_at": {{ now }}
}
""")

nationalities = {
    "hk": {
        "female": ["HKC", "HKL"],
        "male": ["HKC", "HKL"],
    },
    "cn": {
        "female": ["ML"],
        "male": ["ML"],
    },
}

group = audience_id.split("_")[-1]
json_str = template.render(
    audience_id=audience_id,
    audience_list_path=audience_list_path,
    nationalities=nationalities[region][group],
    region=region,
    now=now,
)
config = json.loads(json_str)
config

# COMMAND ----------

config_path = os.path.join(audience_dir, "config.json")
with open(config_path, "w") as f:
    f.write(json.dumps(config, indent=4))

print(f"Writing to {config_path}")
