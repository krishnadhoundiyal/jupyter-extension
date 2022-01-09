import os
import re
from traitlets.config import SingletonConfigurable
import autopep8
from black import FileMode
from black import format_str
from jinja2 import Template
from .dfoperationtest import Operation
from .dfoperationtest import GenericOperation
from .cloudobjectstorage import CosClient
from typing import Dict
from typing import List
from typing import Union
import ast
from .archivetest import create_temp_archive
#from minio.error import SignatureDoesNotMatch
from urllib3.exceptions import MaxRetryError
from .CreateNoteBookEnv import NoteBookOperatorBuilder
class PipelineProcessorManager(SingletonConfigurable):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        #self.root_dir = os.getcwd()
        # TODO - Set up the serving directory from some request attribute injected into the Handler from Jupyter Server
        self.root_dir = "/project/jupyterlab_apod"
        self.out_dir = "/source2/myextension/"

    def export(self, pipeline):
        default_args = {
            'owner': 'airflow_user1',
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            # 'retry_delay': timedelta(minutes=5),
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
            # matches = re.match(regex, items["_type"])
            items["module_name"] = "airflow.operators.notebook"
            items["class_name"] = "NotebookOperator"
            items["task_id"] = items["task_id"]
            items["parameters"] = {
                "task_id": items["task_id"],
                "NoteBookCommands": "Will add them here later"
            }
        with open(os.path.join(self.out_dir,'templates','airflow_dag.jinja2')) as f:
            tmpl = Template(f.read())
        python_output = tmpl.render(json_rules=pipeline, default_args=default_args)
        outFileName = pipeline["name"] + ".py"
        fileOutPath = os.path.join(self.out_dir, outFileName)
        with open(fileOutPath, "w") as fh:
            autopep_output = autopep8.fix_code(python_output)
            output_to_file = format_str(autopep_output, mode=FileMode())
            fh.write(output_to_file)
        return fileOutPath



class PipelineProcessor():  # ABC

    def __init__(self, **kwargs):
        #super().__init__(**kwargs)
        #self.root_dir = os.getcwd()
        # TODO - Set up the serving directory from some request attribute injected into the Handler from Jupyter Server
        self.root_dir = "/project/jupyterlab_apod"
        self.export_path = "/source2/myextension/"

    @property
    def _get_components(self,configuration) -> List:
        component_list = list(map(lambda item: item["id"], configuration))
        return component_list

    def _find_parent(self, component_id,configuration,pipeline) -> List:
        parent_list = []
        config = configuration
        for parents,children in pipeline["connections"].items():
            if list(filter(lambda items: items == component_id, children)):
                parent_list.append(parents)


        return parent_list

    @staticmethod
    def parent_exists(item,connections):
        childNodes = []
        for items, val in connections.items():
            for i in val:
                childNodes.append(i)
        y = filter(lambda compid: compid == item["id"], childNodes)
        if list(y):
            return True
        else:
            return False

    @staticmethod
    def get_parents(item,connections,configuration):
        parent_id = []
        result = []
        for parent, children in connections.items():
            y = filter(lambda temp: temp == item["id"], children)
            if (list(y)):
                parent_id.append(parent)
        for parents in parent_id:
            temp_obj = list(filter(lambda obj: obj["id"] == parents, configuration))
            result.append(temp_obj[0])
        return result

    @staticmethod
    def list_items(someV):
        out = []
        for i in someV:
            out.append(i["outfiles"])
        return out

    @staticmethod
    def _propagate_operation_inputs_outputs(item,connections,configuration):
        """
                All previous operation outputs should be propagated throughout the pipeline.
                In order to process this recursively, the current operation's inputs should be combined
                from its parent's inputs (which, themselves are derived from the outputs of their parent)
                and its parent's outputs.
        """
        if not PipelineProcessor.parent_exists(item,connections):
            return []
        valList = PipelineProcessor.get_parents(item,connections,configuration)
        outVal = []
        for z in valList:
            outVal.extend(z["outputs"])
            val = PipelineProcessor._propagate_operation_inputs_outputs(z,connections,configuration)
            outVal.extend(val)
        return outVal


class RuntimePipelineProcessor(PipelineProcessor):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._configuration = kwargs["configurations"]
        self._pipeline = kwargs["pipeline"]
        self._runtime_configuration = kwargs["runtime_configuration"]

    def create_pipeline_file(self):
        target_ops = self.process_pipeline()
        user_namespace = self._runtime_configuration.get('user_namespace','default')
        cos_secret =self._runtime_configuration.get('cos_password')

        pipeline_description = self._pipeline["pipeline_description"]
        if pipeline_description is None:
            pipeline_description = f"Created with Explorer  pipeline editor using `{self._pipeline['name']}`."
        with open(os.path.join(self.export_path,'templates','airflow_template.jinja2')) as fileHandle:
            tmpl = Template(fileHandle.read())

        python_output = tmpl.render(operations_list=target_ops,
                                        pipeline_name=self._pipeline['name'],
                                        namespace=user_namespace,
                                        cos_secret=cos_secret,
                                        kube_config_path=None,
                                        is_paused_upon_creation='False',
                                        in_cluster='True',
                                        pipeline_description=pipeline_description,
                                        connections=self._pipeline["connections"]
                                    )
        outFileName = self._pipeline["name"] + ".py"
        print (python_output)
        fileOutPath = os.path.join(self.export_path, outFileName)
        with open(fileOutPath, "w") as fh:
            autopep_output = autopep8.fix_code(python_output)
            output_to_file = format_str(autopep_output, mode=FileMode())
            fh.write(output_to_file)
        return fileOutPath

    def process_pipeline(self) -> List:
        target_ops = []
        for items in self._configuration:
            if (items["type"] == "NoteBook"):
                parentids = self._find_parent(items["id"],self._configuration,self._pipeline)
                #operation_artifact_archive = self._get_dependency_archive_name(items)
                # Add outputs of ALL parents of this node as Input to this Node
                items["inputs"] = self._propagate_operation_inputs_outputs(items, self._pipeline["connections"],
                                                                     self._configuration)

                operationItem = Operation.create_instance(items["id"],"NotebookOp",items["id"],
                               "execute-notebook-node",parentids,items)

                operation_artifact_archive = self._get_dependency_archive_name(operationItem)
                pipeline_envs = self._collect_envs(operationItem,
                                                   cos_secret=self._runtime_configuration["cos_password"],
                                                   cos_username=self._runtime_configuration["cos_username"],
                                                   cos_password=self._runtime_configuration["cos_password"])
                pipeline_envs['EXPLORER_RUN_NAME'] = self._runtime_configuration["user_params"]["name"]
                notebookBuilder = NoteBookOperatorBuilder(filename=operationItem.filename,
                                               cos_endpoint=self._runtime_configuration["cos_endpoint"],
                                               cos_bucket=self._runtime_configuration["cos_bucket"],
                                               cos_directory=self._runtime_configuration["user_params"]["name"],
                                               cos_dependencies_archive=operation_artifact_archive,
                                               inputs=operationItem.inputs,
                                               outputs=operationItem.outputs)
                target_op = {#'notebook': operationItem.id,
                             'task_id': "'" + operationItem.id + "'",
                             'id' : operationItem.id,
                             'name' : "'" + operationItem.id + "'",
                             'namespace' : "'" + 'default' + "'",
                             'cmds' : ["sh", "-c"],
                             'arguments': [notebookBuilder.container_cmd],
                             #'runtime_image': operationItem.runtime_image,
                             'image' : "'" + operationItem.runtime_image + "'",
                             'env_vars':pipeline_envs,
                             'config_file' : 'None',
                             #'parent_operation_ids': operationItem.parent_operation_ids,
                             'image_pull_policy': "'" + "Always" + "'"
                             #'operator_source': operationItem.component_params['filename'],
                             #'is_generic_operator': True,
                             #'component_params': operationItem.component_params_as_dict

                             }
                target_ops.append(target_op)
                self._upload_dependencies_to_object_store(self._runtime_configuration["user_params"]["name"],operationItem)

        return target_ops

    def _get_dependency_archive_name(self, operation):
        artifact_name = os.path.basename(operation.filename)
        (name, ext) = os.path.splitext(artifact_name)
        return name + '-' + operation.id + ".tar.gz"

    def _get_dependency_source_dir(self, operation):
        return os.path.join(self.root_dir, os.path.dirname(operation.filename.lstrip(os.path.sep)))

    def _generate_dependency_archive(self, operation):
        archive_artifact_name = self._get_dependency_archive_name(operation)
        archive_source_dir = self._get_dependency_source_dir(operation)

        dependencies = [os.path.basename(operation.filename)]
        # For all items in dependecies, iterate and ensure the correct path is determined
        dependencies_list = list(map(lambda dependent:dependent.lstrip(os.path.sep), operation.dependencies))
        dependencies.extend(dependencies_list)
        archive_artifact = create_temp_archive(archive_name=archive_artifact_name,
                                               source_dir=archive_source_dir,
                                               filenames=dependencies,
                                               recursive=operation.include_subdirectories,
                                               require_complete=True)

        return archive_artifact

    def _upload_dependencies_to_object_store(self, pipeline_name, operation):
        operation_artifact_archive = self._get_dependency_archive_name(operation)
        cos_directory = pipeline_name

        # upload operation dependencies to object store
        try:

            dependency_archive_path = self._generate_dependency_archive(operation)
            cos_client = CosClient(config=self._runtime_configuration)
            cos_client.upload_file_to_dir(dir=cos_directory,
                                          file_name=operation_artifact_archive,
                                          file_path=dependency_archive_path)


        except FileNotFoundError as ex:
            #self.log.error("Dependencies were not found building archive for operation: {}".
                           #format(operation.name), exc_info=True)
            raise FileNotFoundError("Node '{}' referenced dependencies that were not found: {}".
                                    format(operation.name, ex)) from ex
        except MaxRetryError as ex:
             pass
            #self.log.error("Connection was refused when attempting to connect to : {}".
                           #format(cos_endpoint), exc_info=True)
        except BaseException as ex:
            #self.log.error("Error uploading artifacts to object storage for operation: {}".
                           #format(operation.name), exc_info=True)
            raise ex from ex


    def _collect_envs(self, operation, **kwargs) -> Dict:
        """
        Collect the envs stored on the Operation and set the system-defined ELYRA_RUNTIME_ENV
        Note: subclasses should call their superclass (this) method first.
        :return: dictionary containing environment name/value pairs
        """

        envs = operation.env_vars_as_dict()
        envs['RUNTIME_ENV'] = getattr(operation, "type")
        envs['AWS_ACCESS_KEY_ID'] = self._runtime_configuration["cos_username"]
        envs['AWS_SECRET_ACCESS_KEY'] = self._runtime_configuration["cos_password"]
        return envs

        #if 'cos_secret' not in kwargs or not kwargs['cos_secret']:

        #else:  # ensure the "access-key" envs are NOT present...
        #    envs.pop('AWS_ACCESS_KEY_ID', None)
        #    envs.pop('AWS_SECRET_ACCESS_KEY', None)

        # Convey pipeline logging enablement to operation
        #
        return envs

    def _process_dictionary_value(self, value: str) -> Union[Dict, str]:
        """
        For component parameters of type dictionary, the user-entered string value given in the pipeline
        JSON should be converted to the appropriate Dict format, if possible. If a Dict cannot be formed,
        log and return stripped string value.
        """
        if not value:
            return {}

        value = value.strip()
        if value == "None":
            return {}

        converted_dict = None
        if value.startswith('{') and value.endswith('}'):
            try:
                converted_dict = ast.literal_eval(value)
            except Exception:
                pass

        # Value could not be successfully converted to dictionary
        if not isinstance(converted_dict, dict):
            #self.log.debug(f"Could not convert entered parameter value `{value}` to dictionary")
            return value

        return converted_dict

    def _process_list_value(self, value: str) -> Union[List, str]:
        """
        For component parameters of type list, the user-entered string value given in the pipeline JSON
        should be converted to the appropriate List format, if possible. If a List cannot be formed,
        log and return stripped string value.
        """
        if not value:
            return []

        value = value.strip()
        if value == "None":
            return []

        converted_list = None
        if value.startswith('[') and value.endswith(']'):
            try:
                converted_list = ast.literal_eval(value)
            except Exception:
                pass

        # Value could not be successfully converted to list
        if not isinstance(converted_list, list):
            #self.log.debug(f"Could not convert entered parameter value `{value}` to list")
            return value

        return converted_list

config = {
    "jsondag": {
        "name": "Dag_generated_Explorer625b2585-e8cc-4e96-a431-b1f77bea953dds",
        "pipeline_description": "Take This from User",
        "nodes": [
            {
                "name": "3b7cf0d315bf45ea820ed29ee4687ca7",
                "type": "JupyterLab Notebook",
                "task_id": "3b7cf0d315bf45ea820ed29ee4687ca7"
            },
            {
                "name": "4b33b55c85004a1dbad5c3b15e4a0702",
                "type": "JupyterLab Notebook",
                "task_id": "4b33b55c85004a1dbad5c3b15e4a0702"
            },
            {
                "name": "29b1d1bae54d48b5b0b3e66cded365ac",
                "type": "JupyterLab Notebook",
                "task_id": "29b1d1bae54d48b5b0b3e66cded365ac"
            },
            {
                "name": "4b4f06f328ff4a788473eb9281948b81",
                "type": "JupyterLab Notebook",
                "task_id": "4b4f06f328ff4a788473eb9281948b81"
            },
            {
                "name": "3be3433fa22242d1a3fbeeeb7158b571",
                "type": "JupyterLab Notebook",
                "task_id": "3be3433fa22242d1a3fbeeeb7158b571"
            }
        ],
        "connections": {
            "3b7cf0d315bf45ea820ed29ee4687ca7": [
                "4b33b55c85004a1dbad5c3b15e4a0702"
            ],
            "4b33b55c85004a1dbad5c3b15e4a0702": [
                "29b1d1bae54d48b5b0b3e66cded365ac"
            ],
            "29b1d1bae54d48b5b0b3e66cded365ac": [
                "4b4f06f328ff4a788473eb9281948b81",
                "3be3433fa22242d1a3fbeeeb7158b571"
            ]
        }
    },
    "configuration": [
         {
            "id": "3b7cf0d315bf45ea820ed29ee4687ca7",
            "label": "eeee",
            "type": "NoteBook",
            "filename": "rt.py",
            "runtime_image": "Pandas",
            "dependencies": [ "application.py","application.py"],
            "outputs": ["t1"],
             "cpu" : 1,
            "gpu" : 2,
            "memory":1,
            "env_vars": [
                {
                    "EnvironKey": "dd",
                    "EnvironValue": "fff"
                }
            ]
        },
        {
            "id": "4b33b55c85004a1dbad5c3b15e4a0702",
            "label": "ffff",
            "type": "NoteBook",
            "filename": "create_data_from_json.py",
            "runtime_image": "PyTorch",
            "dependencies": ["dag1.py","dag1.py"],

            "outputs": ["p1"],
             "cpu" : 1,
            "gpu" : 2,
            "memory":1,
            "env_vars": [
                {
                    "EnvironKey": "y",
                    "EnvironValue": "kk"
                }
            ]
        },
        {
            "id": "29b1d1bae54d48b5b0b3e66cded365ac",
            "label": "Custome Label for Notebook",
            "type": "NoteBook",
            "filename": "dag1.py",
            "runtime_image": "None",
            "dependencies": ["dd.py","dag1.py"],
            "outputs": ["r1.csv"],
            "cpu" : 1,
            "gpu" : 2,
            "memory":1,
            "env_vars": [
                {
                    "EnvironKey": "d",
                    "EnvironValue": "fdfd"
                }
            ]
        },
         {
            "id": "3be3433fa22242d1a3fbeeeb7158b571",
            "label": "ddd",
            "type": "NoteBook",
            "filename": "package-lock.json",
            "runtime_image": "None",
            "dependencies": ["tsconfig.json"

            ],
            "outputs": [
                 "ll.csv"

            ],
            "env_vars": [
                {
                    "EnvironKey": "ff",
                    "EnvironValue": "wsd"
                }
            ]
        },
        {
            "id": "4b4f06f328ff4a788473eb9281948b81",
            "label": "ffd",
            "type": "NoteBook",
            "filename": "install.json",
            "runtime_image": "None",
            "dependencies": [

                    "RELEASE.md"

            ],
            "outputs": [

                   "tr.csv"

            ],
            "env_vars": [
                {
                    "EnvironKey": "f",
                    "EnvironValue": "d"
                }
            ]
        }
    ],
    "github": {
        "token": "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",
        "repo": "krishnadhoundiyal/extensions-jupyter",
        "branch": "test"
    },
    "cos-config": {
        "cos_endpoint": "https://storage.googleapis.com",
        "cos_username": "GOOGL5U4Z4PN6BCIYVZRQPIZ",
        "cos_password": "0PKhq1+tiY89IzXwwhF5MqXTYefFCfZl8mksiqe8",
        "cos_bucket": "explorer-cloud-storage",
        "user_params" : {
            "name" : "Dag_generated_Explorer625b2585-e8cc-4e96-a431-b1f77bea953dds"
        }

    }
}
config = {
    "jsondag": {
        "name": "Dag_generated_Explorer2b2e8e29-b074-4907-b4c2-15a15bac17cb4343",
        "pipeline_description": "Take This from User",
        "nodes": [
            {
                "name": "3b7cf0d315bf45ea820ed29ee4687ca7",
                "type": "JupyterLab Notebook",
                "task_id": "3b7cf0d315bf45ea820ed29ee4687ca7"
            },

            {
                "name": "29b1d1bae54d48b5b0b3e66cded365ac",
                "type": "JupyterLab Notebook",
                "task_id": "29b1d1bae54d48b5b0b3e66cded365ac"
            },
            {
                "name": "4b4f06f328ff4a788473eb9281948b81",
                "type": "JupyterLab Notebook",
                "task_id": "4b4f06f328ff4a788473eb9281948b81"
            },
            {
                "name": "3be3433fa22242d1a3fbeeeb7158b571",
                "type": "JupyterLab Notebook",
                "task_id": "3be3433fa22242d1a3fbeeeb7158b571"
            },
        ],
        "connections": {
            "3b7cf0d315bf45ea820ed29ee4687ca7": [
                "29b1d1bae54d48b5b0b3e66cded365ac"

            ],
            "29b1d1bae54d48b5b0b3e66cded365ac": [
                "4b4f06f328ff4a788473eb9281948b81",
                "3be3433fa22242d1a3fbeeeb7158b571"
            ]
        }
    },
    "configuration": [
        {
            "id": "3b7cf0d315bf45ea820ed29ee4687ca7",
            "label": "Load-Data",
            "type": "NoteBook",
            "filename": "load_data.ipynb",
            "runtime_image": "Pandas",
            "dependencies": [
            ],
			 "cpu" : 1,
            "gpu" : 2,
            "memory":1,
            "outputs": [
               "data/noaa-weather-data-jfk-airport/jfk_weather.csv"

            ],
            "env_vars": [
                {
                    "EnvironKey": "DATASET_URL",
                    "EnvironValue": "https://dax-cdn.cdn.appdomain.cloud/dax-noaa-weather-data-jfk-airport/1.1.4/noaa-weather-data-jfk-airport.tar.gz"
                }
            ]
        },
        {
            "id": "29b1d1bae54d48b5b0b3e66cded365ac",
            "label": "Custome Label for Notebook",
            "type": "NoteBook",
            "filename": "Part 1 - Data Cleaning.ipynb",
            "runtime_image": "Pandas",
			 "cpu" : 1,
            "gpu" : 2,
            "memory":1,
            "dependencies": [
            ],
            "outputs": [
                "data/noaa-weather-data-jfk-airport/jfk_weather_cleaned.csv"

            ],
            "env_vars": [
                {
                    "EnvironKey": "d",
                    "EnvironValue": "g"
                }
            ]
        },
        {
            "id": "3be3433fa22242d1a3fbeeeb7158b571",
            "label": "Part 3 - Time Series Forecasting.ipynb",
            "type": "NoteBook",
            "filename": "Part 3 - Time Series Forecasting.ipynb",
            "runtime_image": "Pandas",
			 "cpu" : 1,
            "gpu" : 2,
            "memory":1,
            "dependencies": [

            ],
            "outputs": [
            ],
            "env_vars": [
                {
                    "EnvironKey": "t",
                    "EnvironValue": "v"
                }
            ]
        },
        {
            "id": "4b4f06f328ff4a788473eb9281948b81",
            "label": "Part 2 - Data Analysis.ipynb",
            "type": "NoteBook",
            "filename": "Part 2 - Data Analysis.ipynb",
            "runtime_image": "Pandas",
			 "cpu" : 1,
            "gpu" : 2,
            "memory":1,
            "dependencies": [

            ],
            "outputs": [

            ],
            "env_vars": [
                {
                    "EnvironKey": "d",
                    "EnvironValue": "f"
                }
            ]
        }
    ],
     "github": {
        "token": "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",
        "repo": "krishnadhoundiyal/extensions-jupyter",
        "branch": "test"
    },
    "cos-config": {
        "cos_endpoint": "https://storage.googleapis.com",
        "cos_username": "GOOGL5U4Z4PN6BCIYVZRQPIZ",
        "cos_password": "0PKhq1+tiY89IzXwwhF5MqXTYefFCfZl8mksiqe8",
        "cos_bucket": "explorer-cloud-storage",
        "user_params" : {
            "name" : "Dag_generated_Explorer2b2e8e29-b074-4907-b4c2-15a15bac17cb4343"
        }

    }
}
