from tornado import web
from jupyter_server.base.handlers import JupyterHandler
from jupyter_server.extension.handler import ExtensionHandlerJinjaMixin
from jupyter_server.extension.handler import ExtensionHandlerMixin
from .pipelineprocessing import PipelineProcessorManager
from .git import GithubClient
import json
class AirflowExecutorOnRemote(ExtensionHandlerMixin, JupyterHandler):
    
    @web.authenticated
    async def post(self, *args, **kwargs):
        payload = self.get_json_body()
        pipeline_definition = payload['jsondag']
        pipeline_configuration = payload['configuration']
        github_configuration = payload['github']

        #Write Json Dag into a Airflow acceptable python DAG format
        pipeline_exported_path = PipelineProcessorManager.instance().export(
                pipeline_definition
            )
        # Notify someways to show export path completed
        gitClient = GithubClient(github_configuration["token"],github_configuration["repo"],github_configuration["branch"])
        gitClient.upload_dag(pipeline_exported_path,pipeline_definition["name"])
        # Notify someways to show upload GitHub completed.
        json_msg = json.dumps({
            "pipeline" : pipeline_definition,
            "configuration" : pipeline_configuration
        });
        self.set_header("Content-Type", 'application/json')
        self.finish(json_msg)

