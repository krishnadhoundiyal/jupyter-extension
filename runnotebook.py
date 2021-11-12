import glob
import logging
import os
import subprocess
import sys
import time

from abc import ABC, abstractmethod
from packaging import version
from typing import Optional, Any, Type, TypeVar
from urllib.parse import urlparse
import minio
import argparse
# Inputs and Outputs separator character.  If updated,
# same-named variable in _notebook_op.py must be updated!
INOUT_SEPARATOR = ';'
logger = logging.getLogger('explorer')
T = TypeVar('T', bound='FileOpBase')
"""
    Usage as
    python runnotebook.py --endpoint storage.googleapis.com 
    --bucket airflow-notebooks-0123 --directory untitled 
    --dependencies-archive file.tar.gz --file load_data.ipynb 
    --outputs data/noaa-weather-data-jfk-airport/jfk_weather.csv;data/noaa-weather-data-jfk-airport/jfk_weather_cleaned.csv
"""
class FileOpBase(ABC):
    filepath=None
    @classmethod
    def get_instance(cls:Type[T], **kwargs)-> T:
        filepath = kwargs['filepath']
        if '.ipynb' in filepath:
            return NotebookFileOp(**kwargs)
        elif '.py' in filepath:
            return PythonFileOp(**kwargs)
        else:
            raise ValueError('Unsupported file type: {}'.format(filepath))
    def __init__(self,**kwargs:Any)->None:
        self.filepath= kwargs["filepath"]
        self.input_params = kwargs
        #self.cos_endpoint = urlparse(self.input_params.get('endpoint'))
        self.cos_endpoint = self.input_params.get('endpoint')
        self.cos_bucket = self.input_params.get("bucket")
        self.cos_client = minio.Minio(self.cos_endpoint,
                                      access_key=os.getenv('ENV_ACCESS_ID','GOOGL5U4Z4PN6BCIYVZRQPIZ'),
                                      secret_key=os.getenv('ENV_SECRET_ID','0PKhq1+tiY89IzXwwhF5MqXTYefFCfZl8mksiqe8')
                                      )

    @abstractmethod
    def execute(self):
        """Override this behavious in the sub-class - based on type of notebook"""
        raise NotImplementedError("Method 'execut()' should be implemented in the derived class")
    def process_dependencies(self):
        """Downloads all the dependent notebook/python books"""
        archive = self.input_params.get('dependencies-archive')
        self.get_file_from_object_storage(archive)
        inputs = self.input_params.get('input')
        if inputs:
            input_list = inputs.split(INOUT_SEPARATOR)
            for file in input_list:
                self.get_file_from_object_storage(file.strip())
        subprocess.call(['tar','-zvxf',archive])
        Utility.log_operation_info("Dependencies downloaded")

    def get_file_from_object_storage(self,file:str)->None:
        """Utility to download the file"""

        object_to_get = self.get_object_storage_filename(file)
        self.cos_client.fget_object(bucket_name=self.cos_bucket,
                                    object_name=object_to_get,
                                    file_path=file
                                    )
        Utility.log_operation_info(f"Donwloaded the file {file} from object : {object_to_get} , storage {self.cos_bucket}")

    def get_object_storage_filename(self,filename:str)->str:
        #return os.path.join(self.input_params.get("directory",","),filename)
        return self.input_params.get("directory") + "/" + filename

    def put_file_to_object_storage(self, file_to_upload: str, object_name: Optional[str] = None) -> None:
        """Utility function to put files into an object storage
                :param file_to_upload: filename
                :param object_name: remote filename (used to rename)
        """
        object_to_upload = object_name
        if not object_to_upload:
            object_to_upload = file_to_upload
        object_to_upload = self.get_object_storage_filename(object_to_upload)
        self.cos_client.fput_object(bucket_name=self.cos_bucket,
                                    object_name=object_to_upload,
                                    file_path=file_to_upload)
        Utility.log_operation_info(f"File {file_to_upload} uploaded to storage {self.cos_bucket},\
                                    object {object_to_upload}")
    def process_outputs(self)->None:
        """Process outputs
                If outputs have been specified, it will upload the appropriate files to object storage
                This method can be overridden by subclasses, although overrides should first
                call the superclass method.
        """
        outputs = self.input_params.get('outputs')
        if outputs:
            output_list = outputs.split(INOUT_SEPARATOR)
            for file in output_list:
                self.process_output_file(file.strip())
        else:
            Utility.log_operation_info('No outputs found in this operation')
    def process_output_file(self, output_file):
        """Puts the file to object storage.  Handles wildcards and directories. """

        matched_files = [output_file]
        #if self.has_wildcard(output_file):  # explode the wildcarded file
        #    matched_files = glob.glob(output_file)

        for matched_file in matched_files:
            if os.path.isdir(matched_file):
                for file in os.listdir(matched_file):
                    self.process_output_file(os.path.join(matched_file, file))
            else:
                self.put_file_to_object_storage(matched_file)


class Utility(object):
    global pipeline_name
    @classmethod
    def parse_arguments(cls,args)->dict:
        logger.debug("Parsing Arguments.....")
        parser = argparse.ArgumentParser()
        parser.add_argument('-e','--endpoint',dest='endpoint',required=True,help="Cloud Storage EndPoint")
        parser.add_argument('-b','--bucket',dest='bucket',help='Cloud Storage bucket',required=True)
        parser.add_argument('-d','--directory',dest='directory',required=True,help='Directory in Cloud where Storage is located')
        parser.add_argument('-f','--file',dest='filepath',required=True,help='File to be executed')
        parser.add_argument('-o', '--outputs', dest="outputs", help='Files to output to object store', required=False)
        parser.add_argument('-i', '--inputs', dest="inputs", help='Files to pull in from parent node', required=False)
        parser.add_argument('-p', '--parameters', dest="parameters", help='Parameterized Notebook Configuration', type=str, required=False)
        parser.add_argument('-t', '--dependencies-archive', dest="dependencies-archive",
                            help='Archive containing notebook and dependency artifacts', required=True)
        #parser.add_argument('-s', '--scrapbook', dest="scrapbook", help='Store Scrapes', required=True)
        parsed_args = vars(parser.parse_args(args))
        global pipeline_name
        pipeline_name = parsed_args.get('directory')
        return parsed_args
    @classmethod
    def log_operation_info(cls,action:str)->None:
        global pipeline_name
        logger.info(f"'{pipeline_name}':'{action}'")
        return None
class NotebookFileOp(FileOpBase):
    """Perform Notebook File Operation"""
    def execute(self) -> None:
        notebook = os.path.basename(self.filepath)
        notebook_name = notebook.replace('.ipynb','')
        notebook_output = notebook_name+'-out.ipynb'
        notebook_html = notebook_name+'.html'
        try:
            Utility.log_operation_info(f"procesing notebook using 'papermill {notebook} {notebook_output}'")
            import papermill
            import json
            config_params = None

            if self.input_params.get("parameters"):
                config_params=json.loads(self.input_params.get("parameters"))
            papermill.execute_notebook(notebook,notebook_output,parameters=config_params)
            Utility.log_operation_info("Processing completed for the notebook")
            #NotebookFileOp.convert_to_html(notebook_output,notebook_html)
            self.put_file_to_object_storage(notebook_output,notebook)
            #self.put_file_to_storage(notebook_html)
            self.process_outputs()
        except Exception as ex:
            logger.error("Unexpected Error : {}".format(sys.exc_info()[0]))
            raise ex
    @staticmethod
    def convert_to_html(notebook_file: str, html_file: str) -> str:
        import nbconvert
        import nbformat
        Utility.log_operation_info(f"converting notebook {notebook_file} to html {html_file}")
        nb = nbformat.read(notebook_file,as_version=4)
        html_exporter = nbconvert.HTMLExporter()
        data,resources = html_exporter.from_notebook_node(nb)
        with open(html_file,"w") as f:
            f.write(data)
            f.close()
        Utility.log_operation_info(f"completed converting {notebook_file} into html {html_file}")
        return html_file

class PythonFileOp(FileOpBase):
    def execute(self)->None:
        python_script = os.path.basename(self.filepath)
        python_script_name = python_script.replace('.py','')
        python_script_log = python_script_name + ".log"
        try:
            Utility.log_operation_info(f"Exececuting python script '{python_script_name}'")
            with open(python_script_log,"w") as log_file:
                subprocess.run(['python',python_script],stdout=log_file,stderr=subprocess.STDOUT,check=True)
            Utility.log_operation_info("Execution Completed")
            self.put_file_to_object_storage(python_script_log)
            self.process_outputs()
        except Exception as ex:
            logger.error("Unexpected error '{}'".format(sys.exc_info()[0]))
            logger.error("Error Detauls '{}".format(ex))
            raise ex


def main():
    # Configure logger format, level
    logging.basicConfig(format='[%(levelname)1.1s %(asctime)s.%(msecs).03d] %(message)s',
                        datefmt='%H:%M:%S',
                        level=logging.INFO)
    # Setup packages and gather arguments
    input_params = Utility.parse_arguments(sys.argv[1:])
    Utility.log_operation_info("starting operation")

    # Create the appropriate instance, process dependencies and execute the operation
    file_op = FileOpBase.get_instance(**input_params)
    file_op.process_dependencies()
    file_op.execute()
    Utility.log_operation_info("operation completed")
if __name__ == '__main__':
    main()
