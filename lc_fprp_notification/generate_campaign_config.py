# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text("campaign_id", "")
dbutils.widgets.text("version_ids", "female,male")
dbutils.widgets.text("audience_ids", "")
dbutils.widgets.text("region", "")
campaign_id = getArgument("campaign_id")
audience_ids = getArgument("audience_ids").split(",")
version_ids = getArgument("version_ids").split(",")
region = getArgument("region").lower()

print("campaign_id:", campaign_id)
print("audience_ids:", audience_ids)
print("version_ids:", version_ids)

# COMMAND ----------

import os
import time
import datetime
import json
from jinja2 import Template

# COMMAND ----------

campaign_template = Template("""
{
	"campaign": {
		"id": "{{ campaign_id }}",
		"name": "fprp_notification",
		"region": "hk",
		"ab_testing": true,
		"utm_campaign": "3",
		"utm_medium": "email",
		"utm_source": "hk",
		"versions": {{ version_ids | tojson() }},
		"status": "Enabled",
        "updated_at": {{ now }},
        "created_at": {{ now }}
	}
}
""")

# COMMAND ----------

version_template = Template("""
{
    "version": {
        "ab_testing_group": "Standalone",
        "language": "EN",
        "name": "{{ version_id }}",
        "preference": "personalized",
        "subject_line": [
            "Hi"
        ],
        "audience": {
            "include": ["{{ audience_id }}"]
        },
        "product_sorting": {
            "col": 4,
            "row": {{ product_sorting | length }},
            "products": {{ product_sorting | tojson() }}
        },
        "id": "{{ version_id }}",
        "product": {
            "global": {
                "filters": {}
            },
            "bu": {
                {% for bu in all_bus %}"{{ bu }}": {}{% if not loop.last %},{% endif %}
                {% endfor %}
            },
            "list": {
                "type": "csv",
                "path": "{{ item_list_path }}"
            }
        }
    },
    "created_at": {{ now }},
    "id": "{{ version_id }}",
    "name": "{{ version_id }}",
    "status": "Enabled",
    "updated_at": {{ now }}
}
""")

# COMMAND ----------

product_sortings = {
    "hk": {
        "female": [["WW", "LSA", "WW", "LSA"], ["HL", "COS", "HL", "COS"]],
        "male": [["MW", "MSA", "MW", "MSA"], ["HL", "MW", "HL", "MW"]],
    },
    "cn": {
        "female": [["WW", "LSA", "WW", "LSA"], ["HL", "COS", "HL", "COS"]],
        "male": [["MW", "MSA", "MW", "MSA"], ["HL", "MW", "HL", "MW"]],
    }
}

# COMMAND ----------

now = time.time()

campaign_json_str = campaign_template.render(
    campaign_id=campaign_id,
    version_ids=version_ids,
    now=now,
)
campaign_config = json.loads(campaign_json_str)

campaign_dir = f"/Volumes/lc_prd/ml_cdxp_p13n_silver/campaign/{campaign_id}"
campaign_config_path = os.path.join(campaign_dir, "campaign.json")
with open(campaign_config_path, "w") as f:
    f.write(json.dumps(campaign_config, indent=4))

print(f"Writing to {campaign_config_path}")
print(campaign_json_str)

# COMMAND ----------

item_list_path = os.path.join(campaign_dir, "item_list.csv")

for version_id, audience_id in zip(version_ids, audience_ids):
    version_json_str = version_template.render(
        version_id=version_id,
        audience_id=audience_id,
        now=now,
        product_sorting=product_sortings[region][version_id],
        all_bus = list({e for l in product_sortings[region][version_id] for e in l}),
        item_list_path=item_list_path,
    )
    version_config = json.loads(version_json_str) 
    version_config_path = os.path.join(campaign_dir, f"version_{version_id}.json")
    print(f"Writing to {version_config_path}")
    with open(version_config_path, "w") as f:
        json.dump(version_config, f, indent=4)
