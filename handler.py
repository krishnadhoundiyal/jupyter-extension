from tornado import web
from jupyter_server.base.handlers import JupyterHandler
from jupyter_server.extension.handler import ExtensionHandlerJinjaMixin
from jupyter_server.extension.handler import ExtensionHandlerMixin
from .pipelineprocessing import PipelineProcessorManager
from .git import GithubClient
from .PipelineProcessor import RuntimePipelineProcessor
import json
class AirflowExecutorOnRemote(ExtensionHandlerMixin, JupyterHandler):
    
    @web.authenticated
    async def post(self, *args, **kwargs):
        #payload = self.get_json_body()
        #pipeline_definition = payload['jsondag']
        #pipeline_configuration = payload['configuration']
        #github_configuration = payload['github']
        ## The structure of the payload to be recieved
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
                    "runtime_image": "docker.io/amancevice/pandas:1.1.1",
                    "dependencies": [
                    ],
                    "cpu": 1,
                    "gpu": 2,
                    "memory": 1,
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
                    "runtime_image": "docker.io/amancevice/pandas:1.1.1",
                    "cpu": 1,
                    "gpu": 2,
                    "memory": 1,
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
                    "runtime_image": "docker.io/amancevice/pandas:1.1.1",
                    "cpu": 1,
                    "gpu": 2,
                    "memory": 1,
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
                    "runtime_image": "docker.io/amancevice/pandas:1.1.1",
                    "cpu": 1,
                    "gpu": 2,
                    "memory": 1,
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
                "branch": "test",
                "url" : "https://api.github.com"
            },
            "cos-config": {
                "cos_endpoint": "https://storage.googleapis.com",
                "cos_username": "GOOGL5U4Z4PN6BCIYVZRQPIZ",
                "cos_password": "0PKhq1+tiY89IzXwwhF5MqXTYefFCfZl8mksiqe8",
                "cos_bucket": "explorer-cloud-storage",
                "user_params": {
                    "name": "Dag_generated_Explorer2b2e8e29-b074-4907-b4c2-15a15bac17cb4343"
                }

            }
        }

        #Write Json Dag into a Airflow acceptable python DAG format
        ob1 = RuntimePipelineProcessor(configurations=config["configuration"], pipeline=config["jsondag"],
                                       runtime_configuration=
                                       config["cos-config"])
        fileOutPath = ob1.create_pipeline_file()
        dag_name = config["jsondag"]["name"]
        github_configuration = config['github']
        gitClient = GithubClient(github_configuration["token"], github_configuration["repo"],
                                 github_configuration["branch"])
        gitClient.upload_dag(fileOutPath, dag_name)
        github_url = gitClient.get_github_url(api_url=github_configuration["url"],
                                                  repository_name=github_configuration["repo"],
                                                  repository_branch=github_configuration["branch"])
        object_storage_url = config["cos-config"]["cos_endpoint"]
        object_storage_path = config["jsondag"]["name"]
        #pipeline_exported_path = PipelineProcessorManager.instance().export(
        #        pipeline_definition
        #    )
        # Notify someways to show export path completed
        #gitClient = GithubClient(github_configuration["token"],github_configuration["repo"],github_configuration["branch"])
        #gitClient.upload_dag(pipeline_exported_path,pipeline_definition["name"])
        # Notify someways to show upload GitHub completed.
        json_msg = json.dumps({
            'git_url' : f'{github_url}',
            'object_storage_url' : f'{object_storage_url}',
            'object_storage_path': f'{object_storage_path}',
            'object_storage_bucket': f'{config["cos-config"]["cos_bucket"]}'

        });
        self.set_header("Content-Type", 'application/json')
        self.finish(json_msg)

