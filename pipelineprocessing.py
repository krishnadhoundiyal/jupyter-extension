import os
import re
from traitlets.config import SingletonConfigurable
import autopep8
from black import FileMode
from black import format_str
from jinja2 import Template
class PipelineProcessorManager(SingletonConfigurable):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.root_dir = os.getcwd()

    def export(self, pipeline):
        default_args = {
            'owner': 'airflow_user1',
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            #'retry_delay': timedelta(minutes=5),
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
        regex = r"(.*)\.(.*)"
        for items in pipeline["nodes"]:
            #matches = re.match(regex, items["_type"])
            items["module_name"] = "airflow.operators.notebook"
            items["class_name"] = "NotebookOperator"
            items["task_id"] = items["task_id"]
            items["parameters"] = {
                            "task_id": items["task_id"],
                            "NoteBookCommands": "Will add them here later"
                        }
        with open('/source2/myextension/templates/airflow_dag.jinja2') as f:
            tmpl = Template(f.read())
        python_output = tmpl.render(json_rules=pipeline, default_args=default_args)
        outFileName = pipeline["name"] + ".py"
        fileOutPath = os.path.join("/source2/myextension/", outFileName)
        with open(fileOutPath, "w") as fh:
            autopep_output = autopep8.fix_code(python_output)
            output_to_file = format_str(autopep_output, mode=FileMode())
            fh.write(output_to_file)
        return fileOutPath
