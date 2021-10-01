from .app import MyExtension

EXTENSION_NAME = "myextension"

def _jupyter_server_extension_points():
    return [
        {
            "module": "myextension.app",
            "app": MyExtension
        }
    ]
