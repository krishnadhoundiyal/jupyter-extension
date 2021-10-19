from typing import Dict
connections = {
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
config = [
        {
            "id": "3b7cf0d315bf45ea820ed29ee4687ca7",
            "label": "eeee",
            "type": "JupyterLabNotebook",
            "jupyterFilePath": "/setup.py",
            "jupyterNotebookDockerImage": "Pandas",
            "juputerNotebookDependency": [
                {
                    "fileSelected": "/untitled.py"
                },
                {
                    "fileSelected": "/untitled.py"
                }
            ],
            "jupyterNotebookOutFiles": [
                {
                    "outfiles": "t1"
                }
            ],
            "jupyterNotebookEnvironVar": [
                {
                    "EnvironKey": "dd",
                    "EnvironValue": "fff"
                }
            ]
        },
         {
            "id": "4b33b55c85004a1dbad5c3b15e4a0702",
            "label": "ffff",
            "type": "JupyterLabNotebook",
            "jupyterFilePath": "/etc/jupyter/jupyter_server_config.d/simple_ext1.json",
            "jupyterNotebookDockerImage": "PyTorch",
            "juputerNotebookDependency": [
                {
                    "fileSelected": "/style/jupyterlab.jpg"
                },
                {
                    "fileSelected": "/LICENSE"
                }
            ],
            "jupyterNotebookOutFiles": [
                {
                    "outfiles": "p1"
                }
            ],
            "jupyterNotebookEnvironVar": [
                {
                    "EnvironKey": "y",
                    "EnvironValue": "kk"
                }
            ]
        },
         {
            "id": "29b1d1bae54d48b5b0b3e66cded365ac",
            "label": "Custome Label for Notebook",
            "type": "JupyterLabNotebook",
            "jupyterFilePath": "",
            "jupyterNotebookDockerImage": "None",
            "juputerNotebookDependency": [
                {
                    "fileSelected": ""
                }
            ],
            "jupyterNotebookOutFiles": [
                {
                    "outfiles": "gddf"
                }
            ],
            "jupyterNotebookEnvironVar": [
                {
                    "EnvironKey": "",
                    "EnvironValue": ""
                }
            ]
        },
         {
            "id": "3be3433fa22242d1a3fbeeeb7158b571",
            "label": "ddd",
            "type": "JupyterLabNotebook",
            "jupyterFilePath": "/package-lock.json",
            "jupyterNotebookDockerImage": "None",
            "juputerNotebookDependency": [
                {
                    "fileSelected": "/tsconfig.json"
                }
            ],
            "jupyterNotebookOutFiles": [
                {
                    "outfiles": "ll"
                }
            ],
            "jupyterNotebookEnvironVar": [
                {
                    "EnvironKey": "ff",
                    "EnvironValue": "wsd"
                }
            ]
        },
        {
            "id": "4b4f06f328ff4a788473eb9281948b81",
            "label": "ffd",
            "type": "JupyterLabNotebook",
            "jupyterFilePath": "/install.json",
            "jupyterNotebookDockerImage": "None",
            "juputerNotebookDependency": [
                {
                    "fileSelected": "/RELEASE.md"
                }
            ],
            "jupyterNotebookOutFiles": [
                {
                    "outfiles": "tr"
                }
            ],
            "jupyterNotebookEnvironVar": [
                {
                    "EnvironKey": "f",
                    "EnvironValue": "d"
                }
            ]
        }
    ]
def parent_exists(item):
    childNodes = []
    for items, val in connections.items():
        for i in val:
            childNodes.append(i)
    y = filter(lambda compid: compid == item["id"],childNodes)
    if list(y):
        return True
    else:
        return False
def get_parents(item):
    parent_id = []
    result = []
    for parent,children in connections.items():
        y = filter(lambda temp : temp == item["id"],children)
        if (list(y)):
            parent_id.append(parent)
    for parents in parent_id:
        temp_obj = list(filter(lambda obj: obj["id"] == parents,config))
        result.append(temp_obj[0])
    return result
def list_items(someV):
    out = []
    for i in someV:
        out.append(i["outfiles"])
    return out
def find_parent(item):

    if not parent_exists(item):
        return []
    valList = get_parents(item)
    outVal = []
    for z in valList:
        outVal.extend(list_items(z["jupyterNotebookOutFiles"]))
        val = find_parent(z)
        outVal.extend(val)
    return outVal
ss = find_parent(config[1])
print(list(ss))
