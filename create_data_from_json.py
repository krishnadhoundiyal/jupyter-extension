from datetime import timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
import re
from airflow.utils.dates import days_ago
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
json_rules = {
                "name": "dag_runtime_generated_id",
                "nodes": [
                    {
                        "name": "Start",
                        "_type": "airflow.operators.dummy.DummyOperator",
                        "parameters": {
                            "task_id" : "1234343"
                        },
                    },
                    {
                        "name": "ExecuteCommand1",
                        "_type": "airflow.operators.bash.BashOperator",
                        "parameters": {
                            "task_id": "123434er",
                            "bash_command": "echo test11"
                        },
                    },
                    {
                        "name": "End",
                        "_type": "airflow.operators.dummy.DummyOperator",
                        "parameters": {
                                "task_id" : "1234343465",
                        },
                    },
                    {
                        "name": "ExecuteCommand",
                        "_type": "airflow.operators.bash.BashOperator",
                        "parameters": {
                            "task_id" : "1234343467",
                            "bash_command": "echo test"
                        },
                    },
                    {
                        "name": "ExecuteCommandx",
                        "_type": "airflow.operators.bash.BashOperator",
                        "parameters": {
                            "task_id" : "level3",
                            "bash_command": "echo test"
                        },
                    },
                    {
                        "name": "ExecuteCommandx-x1",
                        "_type": "airflow.operators.bash.BashOperator",
                        "parameters": {
                            "task_id" : "level3_1",
                            "bash_command": "echo test"
                        },
                    },
                    {
                        "name": "ExecuteCommandx1",
                        "_type": "airflow.operators.bash.BashOperator",
                        "parameters": {
                            "task_id" : "level4",
                            "bash_command": "echo test"
                        },
                    },
                    {
                        "name": "ExecuteCommandx-x1-x1",
                        "_type": "airflow.operators.bash.BashOperator",
                        "parameters": {
                            "task_id" : "level3_1_1",
                            "bash_command": "echo test"
                        },
                    },
                    {
                        "name": "ExecuteCommandx-x1-x2",
                        "_type": "airflow.operators.bash.BashOperator",
                        "parameters": {
                            "task_id" : "level3_1_2",
                            "bash_command": "echo test"
                        },
                    },
                    {
                        "name": "ExecuteCommandx-x1-x2-end",
                        "_type": "airflow.operators.bash.BashOperator",
                        "parameters": {
                            "task_id" : "level3_1_2-end",
                            "bash_command": "echo test"
                        },
                    },
                    {
                        "name": "ExecuteCommandx11-x1",
                        "_type": "airflow.operators.bash.BashOperator",
                        "parameters": {
                            "task_id" : "level4_1",
                            "bash_command": "echo test"
                        },
                    },
                ],
                "connections": {
                    "Start": ["ExecuteCommand", "ExecuteCommand1"],
                    "ExecuteCommand1": ["ExecuteCommandx","ExecuteCommandx1"],
                    "ExecuteCommandx" : ["ExecuteCommandx-x1"],
                    "ExecuteCommandx-x1" : ["ExecuteCommandx-x1-x1","ExecuteCommandx-x1-x2"],
                    "ExecuteCommandx-x1-x1" : ["ExecuteCommandx-x1-x2-end"],
                    "ExecuteCommandx-x1-x2" : ["ExecuteCommandx-x1-x2-end"],
                    "ExecuteCommandx-x1-x2-end" : ["End"],

                    "ExecuteCommandx1": ["ExecuteCommandx11-x1"],
                    "ExecuteCommandx11-x1" : ["End"],
                    "ExecuteCommand": ["End"]
                }
            }
import json
#tasks_items = json.load(json_rules)
tasks_items = json_rules
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
# Operators; we need this to operate!
def load_operator(name):
    regex = r"(.*)\.(.*)"
    matches = re.match(regex,name)
    mod = __import__(matches[1],fromlist=[matches[2]])
    klass = getattr(mod,matches[2])
    return klass
def create_data(schedule=None,default_args={},**kwargs):
    tasks_items = kwargs
    with DAG(
        tasks_items["name"],
        default_args=default_args,
        description='Dynamically generated DAG',
        schedule_interval=timedelta(days=1),
        start_date=days_ago(2),
        tags=['example'],
    ) as dag:
        task_lookup = {}
        for items in json_rules["nodes"]:
            operatorClass = load_operator(items["_type"])
            params = items["parameters"]
            params["dag"] = dag
            task_lookup[items["name"]] = operatorClass(**params)
        for source,destlist in json_rules["connections"].items():
            for downstream in destlist:
                task_lookup[source] >> task_lookup[downstream]
        #for items in json_rules["connections"]:
        #    for down_items in items:
        #        task_lookup[items["name"]] >> task_lookup[down_items]

        return dag
globals()["{},task_name".format(tasks_items["name"])] = create_data(None,default_args,**json_rules)






