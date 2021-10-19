{
    "name": "Dag Generted Explorer",
    "pipeline_description": "Take This from User",
    "nodes": [
        {
            "name": "f4c17de4-2003-42c0-9da9-13bf4f363480",
            "type": "JupyterLab Notebook",
            "task_id": "f4c17de4-2003-42c0-9da9-13bf4f363480"
        },
        {
            "name": "8bf8349f-2d8f-44a5-9454-4161d72e69a2",
            "type": "JupyterLab Notebook",
            "task_id": "8bf8349f-2d8f-44a5-9454-4161d72e69a2"
        },
        {
            "name": "8832713a-3f4c-4741-986e-ae6d769c91e3",
            "type": "JupyterLab Notebook",
            "task_id": "8832713a-3f4c-4741-986e-ae6d769c91e3"
        },
        {
            "name": "fd1786f6-b735-4e66-91f7-9b43fd9385ba",
            "type": "JupyterLab Notebook",
            "task_id": "fd1786f6-b735-4e66-91f7-9b43fd9385ba"
        },
        {
            "name": "0cbe4355-6bc2-4552-9644-488b020147d5",
            "type": "JupyterLab Notebook",
            "task_id": "0cbe4355-6bc2-4552-9644-488b020147d5"
        },
        {
            "name": "0d4aa4fc-46b0-41f8-8842-28c023361e49",
            "type": "JupyterLab Notebook",
            "task_id": "0d4aa4fc-46b0-41f8-8842-28c023361e49"
        },
        {
            "name": "9d7acd2e-b4f3-4060-9739-02fd2282354e",
            "type": "JupyterLab Notebook",
            "task_id": "9d7acd2e-b4f3-4060-9739-02fd2282354e"
        },
        {
            "name": "39c99121-4620-4b51-9dcf-3c61e7fab77e",
            "type": "Python Script",
            "task_id": "39c99121-4620-4b51-9dcf-3c61e7fab77e"
        }
    ],
    "connections": {
        "f4c17de4-2003-42c0-9da9-13bf4f363480": [
            "8bf8349f-2d8f-44a5-9454-4161d72e69a2",
            "8832713a-3f4c-4741-986e-ae6d769c91e3",
            "0cbe4355-6bc2-4552-9644-488b020147d5"
        ],
        "8bf8349f-2d8f-44a5-9454-4161d72e69a2": [
            "fd1786f6-b735-4e66-91f7-9b43fd9385ba"
        ],
        "8832713a-3f4c-4741-986e-ae6d769c91e3": [
            "fd1786f6-b735-4e66-91f7-9b43fd9385ba"
        ],
        "0cbe4355-6bc2-4552-9644-488b020147d5": [
            "0d4aa4fc-46b0-41f8-8842-28c023361e49"
        ],
        "fd1786f6-b735-4e66-91f7-9b43fd9385ba": [
            "9d7acd2e-b4f3-4060-9739-02fd2282354e"
        ]
    }
}