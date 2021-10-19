import os
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.decorators import apply_defaults
from typing import Dict, List, Optional
from airflow.kubernetes.secret import Secret
INOUT_SEPARATOR = ';'
class NotebookOp(KubernetesPodOperator):
    @apply_defaults
    def __init__(self,
                 notebook: str,
                 endpoint: str,
                 bucket: str,
                 directory: str,
                 dependencies: str,
                 pipeline_output: Optional[List[str]]= None,
                 pipeline_inputs: Optional[List[str]] = None,
                 pipeline_config_params: Optional[Dict] = None,
                 requirements_url: str = None,
                 bootstrap_script_url: str = None,
                 scrapbook: str= None,
                 *args,
                 **kwargs) -> None :
        """
        :param notebook: Name of the notebook that will be executed
        :param endpoint: URL of the Cloud where the Notebook will be available
        :param bucket: Bucket S3/GC
        :param directory: Cloud Storage Directory containing
        :param dependencies: Dependent Notebooks archived on the Cloud Storage
        :param pipeline_output:omma delimited list of files produced
        :param pipeline_inputs:omma delimited list of files required by the notebook
        :param requirements_url: Container environment set up python modules
        :param bootstrap_script_url:
        :param scrapbook:
        :param args:
        :param kwargs:
        """
        self.notebook = notebook
        self.notebook_name = self.notebook_name = os.path.basename(notebook)
        self.cos_endpoint = endpoint
        self.cos_bucket = bucket
        self.cos_directory = directory
        self.cos_dependencies_archive = dependencies
        self.container_work_dir_root_path = "./"
        self.container_work_dir_name = "jupyter-work-dir/"
        self.container_work_dir = self.container_work_dir_root_path + self.container_work_dir_name
        self.bootstrap_script_url = bootstrap_script_url
        self.requirements_url = requirements_url
        self.pipeline_outputs = pipeline_output
        self.pipeline_inputs = pipeline_inputs
        self.pipeline_config_params = pipeline_config_params
        self.scrapbook = scrapbook

        # Create the Secret Object to load the Credentials to access the Cloud Storage
    def container_cmd(self):
        """ container_commands = create directory   - mkdir -p self.container_work_dir
                                  env_vars will be passed which will have the needed arugments to
                                  connect to GC or S3
                                  pass arguments to bootstrap program in available in the docker file
                                  -- notebook, notebook_name, endpoint, bucket, dependencies
                                  scrapbook, inputs and outputs
        NOTE: Images being pulled must have python3 available on PATH and cURL utility
        """
        argument_list = [];
        argument_list.append("mkdir -p {self.container_work_dir} && cd {self.container_work_dir} && "
                             "curl -H 'Cache-Control: no-cache' -L {reqs_url} --output requirements.txt &&)"
                             "python3 -m pip install requirements.txt && "
                             "copy /src/runnotebook.py ."
                             "python3 runnotebook.py "
                             "--endpoint {cloud_endpoint}"
                             "--bucket {cloud_bucket}"
                             "--directory '{cloud_directory}'"
                             "--archive '{cloud_archive}'"
                             "--file '{notebook}'"
                             .format(container_work_dir=self.container_work_dir,
                                     reqs_url=self.requirements_url,
                                     cloud_endpoint=self.cos_endpoint,
                                     cloud_cloud_archive=self.cos_dependencies_archive,
                                     cloud_bucket=self.cos_bucket,
                                     cloud_directory=self.cos_directory,
                                     notebook=self.notebook
                                     )

                             )
        if self.pipeline_inputs:
            inputs_str = self._artifact_list_to_str(self.pipeline_inputs)
            argument_list.append("--inputs '{}' ".format(inputs_str))

        if self.pipeline_outputs:
            outputs_str = self._artifact_list_to_str(self.pipeline_outputs)
            argument_list.append("--outputs '{}' ".format(outputs_str))
        if self.pipeline_config_params:
            argument_list.append("--parameters '{}'".format(self.pipeline_config_params))

    def _artifact_list_to_str(self, pipeline_array):
        trimmed_artifact_list = []
        for artifact_name in pipeline_array:
            if INOUT_SEPARATOR in artifact_name:  # if INOUT_SEPARATOR is in name, throw since this is our separator
                raise \
                    ValueError(
                        "Illegal character ({}) found in filename '{}'.".format(INOUT_SEPARATOR, artifact_name))
            trimmed_artifact_list.append(artifact_name.strip())
        return INOUT_SEPARATOR.join(trimmed_artifact_list)