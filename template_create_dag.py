from datetime import timedelta
from jinja2 import Template
import re
import json
import argparse
import sys
import autopep8
from black import FileMode
from black import format_str
from jinja2 import Environment
from jinja2 import PackageLoader
default_args = {
    'owner': 'airflow_user1',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

def read_json_to_dict(json_path:str)->dict:
    task_items = json.load(open(json_path,'r'))
    return task_items
parser = argparse.ArgumentParser()
parser.add_argument('-f','--jsonpath',dest='jsonpath',required=True,help="location where json is located")
parser.add_argument('-o','--dagfile',dest='dagfile',required=True,help="dag location and name")
parsed_args = vars(parser.parse_args(sys.argv[1:]))

task_items = read_json_to_dict(parsed_args["jsonpath"])
regex = r"(.*)\.(.*)"
for items in task_items["nodes"]:
    matches = re.match(regex, items["_type"])
    items["module_name"] = matches[1]
    items["class_name"] = matches[2]
    items["task_id"] = items["parameters"]["task_id"]
with open('airflow_dag.jinja2') as f:
    tmpl = Template(f.read())
python_output = tmpl.render(json_rules=task_items,default_args=default_args)
with open(parsed_args["dagfile"], "w") as fh:
    autopep_output = autopep8.fix_code(python_output)
    output_to_file = format_str(autopep_output, mode=FileMode())
    fh.write(output_to_file)


