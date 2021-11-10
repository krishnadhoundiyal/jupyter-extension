import requests
import uuid
import base64
import json
class TriggerAirflow():
    def __init__(self,**kwargs):
        self.airflow_ip = kwargs["Airflow_IP"]
        self.airflow_port = kwargs["Airflow_Port"]
        self.airflow_uname = kwargs["Airflow_Uname"]
        self.airflow_pass = kwargs["Airflow_Pass"]
    def trigger_dag(self,dag_id):
        dag_run_id = dag_id + "-" + uuid.uuid4().hex
        payload = {
            "dag_run_id" : "Dag_generated_Explorer2b2e8e29-b074-4907-b4c2-15a15bac17cb4343-1211",
            "conf" : {}
        }
        payload = json.dumps(payload)
        headers = {
            'Content-type': 'application/json',
            'Authorization': 'Basic ' + self._encode_upass()

        }
        url = "http://"+ self.airflow_ip + ":"+ self.airflow_port +"/api/v1/dags/" + dag_id + "/dagRuns"
        try:
            response = requests.request("POST", url, headers=headers, data=payload)
            data = response.json()
        #response = data.decode("utf-8")
            if (data['status'] != 200) :
                if (data['title'] == "Conflict"):
                    raise RuntimeError("A Dag Run Id already exists, try and re-execute " +
                                       "If problem persists, contact System Admin")
        except requests.exceptions.HTTPError as errh:
            raise RuntimeError("HTTP Request encountered an error") from errh

        except requests.exceptions.ConnectionError as errh:
            raise RuntimeError("Connecting to IP - {ip} and port  - {port} using  - {creds} credentials \
             ".format(ip=self.airflow_ip, port= self.airflow_port, creds=self.airflow_uname+":"+self.airflow_pass)+ " validate the configuration \
             are correct") from errh

        except requests.exceptions.Timeout as errh:
            raise RuntimeError("Timeout Connecting to IP - {ip} and port  - {port} using  - {creds} credentials \
                         ".format(ip=self.airflow_ip, port=self.airflow_port,
                                  creds=self.airflow_uname + ":" + self.airflow_pass) + " validate the configuration \
                         are correct") from errh
        except BaseException as errh:
            raise errh from errh
    def get_status(self,dag, dag_run_id):
        url = "http://" + self.airflow_ip + ":" + self.airflow_port + "/api/v1/dags/" + \
              dag + "/dagRuns/" + dag_run_id + "/taskInstances"

        payload = {}
        headers = {
            'Authorization': 'Basic ' + self._encode_upass()
        }
        # default data value
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
            # Ignore the error and send something UI can ignore
            # raise errh from errh
            pass
        return data
    def get_logs(self,dag_id,dag_run_id,task_id):
        url = "http://" + self.airflow_ip + ":" + self.airflow_port + "/api/v1/dags/" + \
              dag_id + "/dagRuns/" + dag_run_id + "/taskInstances/" + task_id + \
              "/logs/1"

        payload = {}
        headers = {
            'Authorization': 'Basic ' + self._encode_upass()
        }
        log = ""
        try:
            response = requests.request("GET", url, headers=headers, data=payload)

            if (response.status_code != 200):
                data = response.json()
                raise RuntimeError("Error fetching the logs - {title}".format(title=data["title"]))
            else:
                data = response.text
        except requests.exceptions.Timeout as errh:
            raise RuntimeError("Timeout Connecting to IP - {ip} and port  - {port} \
                         ".format(ip=self.airflow_ip, port=self.airflow_port,
                                  creds=self.airflow_uname + ":" + self.airflow_pass) ) from errh
        except BaseException as errh:
            # Ignore the error and send something UI can ignore
            # raise errh from errh
            # some testing
            import pdb;pdb.set_trace()
            raise errh from errh
        return data
        # default data value
    def _encode_upass(self):
        credentials = self.airflow_uname + ":" + self.airflow_pass
        message_bytes = credentials.encode('ascii')
        base64_bytes = base64.b64encode(message_bytes)
        base64_message = base64_bytes.decode('ascii')
        return base64_message
if __name__ == "__main__":
    obj1 = TriggerAirflow(Airflow_IP="10.174.134.231",Airflow_Port='30507',
                          Airflow_Uname="admin",Airflow_Pass="admin")
    #obj1.trigger_dag("Dag_generated_Explorer2b2e8e29-b074-4907-b4c2-15a15bac17cb4343")
    #obj1.get_status("Dag_generated_Explorer2b2e8e29-b074-4907-b4c2-15a15bac17cb4343","Dag_generated_Explorer2b2e8e29-b074-4907-b4c2-15a15bac17cb4343-12")
    obj1.get_logs("Dag_generated_Explorer2b2e8e29-b074-4907-b4c2-15a15bac17cb4343-1",
                  "Dag_generated_Explorer2b2e8e29-b074-4907-b4c2-15a15bac17cb4343",
                  "3b7cf0d315bf45ea820ed29ee4687ca7")
