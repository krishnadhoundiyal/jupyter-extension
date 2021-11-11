import os
import jinja2
from jupyter_server.extension.application import ExtensionApp
from .handler import AirflowExecutorOnRemote
from .AirflowTaskStatus import TriggerAirflow
from .AirflowTaskStatus import AirflowTaskStatus
from .AirflowTaskStatus import AirflowLogStatus
from .AirflowTaskStatus import AirflowDAGStatus
from .AirflowTaskStatus import AirflowJupyterNotebookDownload
HERE = os.path.dirname(__file__)

class MyExtension(ExtensionApp):
    # Name of the extension.
    extension_name = "my_ext"

    # Local path to static files directory.
    static_paths = [ os.path.join(HERE,'static') ]

    # Default to the extension's URL.
    default_url = "/explorersdev"

    def initialize_templates(self):
        pass
        # Local path to templates directory.
        #template_paths = [os.path.join(HERE,'templates')]
        #env = jinja2.Environment(loader=jinja2.FileSystemLoader(template_paths))
        #template_settings={'myext_jinja2_env':env}
        #self.settings.update(**template_settings)

    def initialize_handlers(self):
        # Add a group with () to send to handler.
        self.handlers.extend([
            (r'/explorersdev/executeAirflow/?', AirflowExecutorOnRemote),
            (r'/explorersdev/getTaskStatus/?', AirflowTaskStatus),
            (r'/explorersdev/getTaskLog/?', AirflowLogStatus),
            (r'/explorersdev/triggerAirflow/?', TriggerAirflow),
            (r'/explorersdev/getDAGStatus/?', AirflowDAGStatus),
            (r'/explorersdev/getProcessedNotebook/?', AirflowJupyterNotebookDownload),
        ])
main = MyExtension.launch_instance
