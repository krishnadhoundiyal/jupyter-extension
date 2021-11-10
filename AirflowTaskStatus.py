from tornado import web
from jupyter_server.base.handlers import JupyterHandler
from jupyter_server.extension.handler import ExtensionHandlerJinjaMixin
from jupyter_server.extension.handler import ExtensionHandlerMixin
from .httperrors import HttpErrorMixin
import requests
import base64
import json
import uuid
class TriggerAirflow(HttpErrorMixin, JupyterHandler):
    @web.authenticated
    async def post(self, *args, **kwargs):
        payload = self.get_json_body()
        airflow_ip = payload['airflow_ip']
        airflow_port = payload['airflow_port']
        airflow_uname = payload['airflow_uname']
        airflow_pass = payload['airflow_pass']
        dag_id = payload['dag_id']
        #config = {
        #    'airflow_ip': '',
        #    'airflow_port': '',
        #    'airflow_uname': '',
        #    'airflow_pass': '',
        #   'dag_id': '',
        #}
        #airflow_ip = config['airflow_ip']
        #airflow_port = config['airflow_port']
        #airflow_uname = config['airflow_uname']
        #airflow_pass = config['airflow_pass']
        #dag_id = config['dag_id']
        credentials = airflow_uname + ":" + airflow_pass
        message_bytes = credentials.encode('ascii')
        base64_bytes = base64.b64encode(message_bytes)
        base64_message = base64_bytes.decode('ascii')
        dag_run_id = dag_id + "-" + uuid.uuid4().hex
        payload = {
            "dag_run_id" : dag_run_id,
            "conf" : {}
        }
        payload = json.dumps(payload)
        headers = {
            'Content-type': 'application/json',
            'Authorization': 'Basic ' + base64_message

        }
        url = airflow_ip + "/api/v1/dags/" + dag_id + "/dagRuns"
        try:
            response = requests.request("POST", url, headers=headers, data=payload)
            data = response.json()
            print(data)
            if data.get("state"):
                #Airflow accepted the request and returned the object
                json_msg = json.dumps({
                    'data': data,
                    'dag_run_id': dag_run_id,
                    'dag_id': dag_id

                });
                self.set_header("Content-Type", 'application/json')
                self.finish(json_msg)
            else:
               #response = data.decode("utf-8")
                if (data.get('status') != 200) :
                    if (data['title'] == "Conflict"):
                        raise RuntimeError("A Dag Run Id already exists, try and re-execute " +
                                           "If problem persists, contact System Admin")
                    else:
                        raise RuntimeError(" detailas are {detail}".format(detail=data["detail"]) +
                                           " Description is {title} ".format(title=data["title"]))
        except requests.exceptions.HTTPError as errh:
            raise RuntimeError("HTTP Request encountered an error") from errh

        except requests.exceptions.ConnectionError as errh:
            raise RuntimeError("Connecting to IP - {ip} and port  - {port} using  - {creds} credentials \
             ".format(ip=airflow_ip, port= airflow_port, creds=airflow_uname+":"+ airflow_pass)+ " validate the configuration \
             are correct") from errh

        except requests.exceptions.Timeout as errh:
            raise RuntimeError("Timeout Connecting to IP - {ip} and port  - {port} using  - {creds} credentials \
                         ".format(ip=airflow_ip, port=airflow_port,
                                  creds=airflow_uname + ":" + airflow_pass) + " validate the configuration \
                         are correct") from errh
        except BaseException as errh:
            raise errh from errh

class AirflowTaskStatus(HttpErrorMixin, JupyterHandler):

    @web.authenticated
    async def post(self, *args, **kwargs):
        payload = self.get_json_body()
        airflow_ip = payload['airflow_ip']
        airflow_port = payload['airflow_port']
        airflow_uname = payload['airflow_uname']
        airflow_pass = payload['airflow_pass']
        dag_id = payload['dag_id']
        dag_run_id = payload['dag_run_id']
        #config = {
        #    'airflow_ip' : '',
        #    'airflow_port' : '',
        #    'airflow_uname' : '',
        #    'airflow_pass' : '',
        #    'dag_id' : '',
        #    'dag_run_id' : ''
        #}
        #airflow_ip = config['airflow_ip']
        #airflow_port = config['airflow_port']
        #airflow_uname = config['airflow_uname']
        #airflow_pass = config['airflow_pass']
        #dag_id = config['dag_id']
        #dag_run_id = config['dag_run_id']
        credentials = airflow_uname + ":" + airflow_pass
        message_bytes = credentials.encode('ascii')
        base64_bytes = base64.b64encode(message_bytes)
        base64_message = base64_bytes.decode('ascii')
        url = airflow_ip  + "/api/v1/dags/" + \
              dag_id + "/dagRuns/" + dag_run_id + "/taskInstances"

        payload = {}
        headers = {
            'Authorization': 'Basic ' + base64_message
        }
        data = []
        try:
            response = requests.request("GET", url, headers=headers, data=payload)
            temp_data = response.json()
            for items in temp_data["task_instances"]:
                vals = {}
                vals["task_id"] = items["task_id"]
                vals["state"] = items["state"]
                data.append(vals)
        except BaseException as errh:
            raise errh from errh
        json_msg = json.dumps({
            'data': data,
            'dag_run_id': dag_run_id,
            'dag_id' : dag_id

        });
        self.set_header("Content-Type", 'application/json')
        self.finish(json_msg)
class AirflowLogStatus(HttpErrorMixin, JupyterHandler):

    @web.authenticated
    async def post(self, *args, **kwargs):
        payload = self.get_json_body()
        airflow_ip = payload['airflow_ip']
        airflow_port = payload['airflow_port']
        airflow_uname = payload['airflow_uname']
        airflow_pass = payload['airflow_pass']
        dag_id = payload['dag_id']
        dag_run_id = payload['dag_run_id']
        task_id = payload['task_id']
        #config = {
        #    'airflow_ip' : '',
        #    'airflow_port' : '',
        #    'airflow_uname' : '',
        #    'airflow_pass' : '',
        #    'dag_id' : '',
        #    'dag_run_id' : '',
        #    'task_id' : ''
        #}
        #airflow_ip = config['airflow_ip']
        #airflow_port = config['airflow_port']
        #airflow_uname = config['airflow_uname']
        #airflow_pass = config['airflow_pass']
        #dag_id = config['dag_id']
        #dag_run_id = config['dag_run_id']
        #task_id = config['task_id']
        credentials = airflow_uname + ":" + airflow_pass
        message_bytes = credentials.encode('ascii')
        base64_bytes = base64.b64encode(message_bytes)
        base64_message = base64_bytes.decode('ascii')
        url =  airflow_ip  + "/api/v1/dags/" + \
              dag_id + "/dagRuns/" + dag_run_id + "/taskInstances/" + task_id + \
              "/logs/1"

        payload = {}
        headers = {
            'Authorization': 'Basic ' + base64_message
        }
        log = ""
        try:
            response = requests.request("GET", url, headers=headers, data=payload)

            if (response.status_code != 200):
                data = response.json()
                raise RuntimeError("Error fetching the logs - {title}".format(title=data["title"]))
            else:
                log = response.text
        except requests.exceptions.Timeout as errh:
            raise RuntimeError("Timeout Connecting to IP - {ip} and port  - {port} \
                                 ".format(ip=airflow_ip, port=airflow_port,
                                          creds=airflow_uname + ":" + airflow_pass)) from errh
        except BaseException as errh:
            raise errh from errh
        json_msg = json.dumps({
            'data': log,
            'dag_run_id': dag_run_id,
            'dag_id' : dag_id

        });
        self.set_header("Content-Type", 'application/json')
        self.finish(json_msg)

