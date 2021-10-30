from logging import Logger
import os
import sys
from typing import Any
from typing import Dict
from typing import List
from typing import Optional


class Operation(object):
    """
    Represents a single operation in a pipeline representing a third-party component
    """

    generic_node_types = ["execute-notebook-node", "execute-python-node", "execute-r-node"]

    @classmethod
    def create_instance(cls, id: str, type: str, name: str, classifier: str,
                        parent_operation_ids: Optional[List[str]] = None,
                        component_params: Optional[Dict[str, Any]] = None) -> 'Operation':
        """Class method that creates the appropriate instance of Operation based on inputs. """

        if classifier in Operation.generic_node_types:
            return GenericOperation(id, type, name, classifier,
                                    parent_operation_ids=parent_operation_ids, component_params=component_params)
        return Operation(id, type, name, classifier,
                         parent_operation_ids=parent_operation_ids, component_params=component_params)

    def __init__(self, id: str, type: str, name: str, classifier: str,
                 parent_operation_ids: Optional[List[str]] = None,
                 component_params: Optional[Dict[str, Any]] = None):
        """
        :param id: Generated UUID, 128 bit number used as a unique identifier
                   e.g. 123e4567-e89b-12d3-a456-426614174000
        :param type: The type of node e.g. execution_node
        :param classifier: indicates the operation's class
        :param name: The name of the operation
        :param parent_operation_ids: List of parent operation 'ids' required to execute prior to this operation
        :param component_params: dictionary of parameter key:value pairs that are used in the creation of a
                                 a non-standard operation instance
        """

        # Validate that the operation has all required properties
        if not id:
            raise ValueError("Invalid pipeline operation: Missing field 'operation id'.")
        if not type:
            raise ValueError("Invalid pipeline operation: Missing field 'operation type'.")
        if not classifier:
            raise ValueError("Invalid pipeline operation: Missing field 'operation classifier'.")
        if not name:
            raise ValueError("Invalid pipeline operation: Missing field 'operation name'.")

        self._id = id
        self._type = type
        self._classifier = classifier
        self._name = name
        self._parent_operation_ids = parent_operation_ids or []
        self._component_params = component_params

        # Scrub the inputs and outputs lists
        self._component_params["inputs"] = Operation._scrub_list(component_params.get('inputs', []))
        self._component_params["outputs"] = Operation._scrub_list(component_params.get('outputs', []))

    @property
    def id(self) -> str:
        return self._id

    @property
    def type(self) -> str:
        return self._type

    @property
    def classifier(self) -> str:
        return self._classifier

    @property
    def name(self) -> str:
        return self._name

    @property
    def parent_operation_ids(self) -> List[str]:
        return self._parent_operation_ids

    @property
    def component_params(self) -> Optional[Dict[str, Any]]:
        return self._component_params

    @property
    def component_params_as_dict(self) -> Dict[str, Any]:
        return self._component_params or {}

    @property
    def inputs(self) -> Optional[List[str]]:
        return self._component_params.get('inputs')

    @inputs.setter
    def inputs(self, value: List[str]):
        self._component_params['inputs'] = value

    @property
    def outputs(self) -> Optional[List[str]]:
        return self._component_params.get('outputs')

    @outputs.setter
    def outputs(self, value: List[str]):
        self._component_params['outputs'] = value

    def __eq__(self, other: 'Operation') -> bool:
        if isinstance(self, other.__class__):
            return self.id == other.id and \
                   self.type == other.type and \
                   self.classifier == other.classifier and \
                   self.name == other.name and \
                   self.parent_operation_ids == other.parent_operation_ids and \
                   self.component_params == other.component_params
        return False

    def __str__(self) -> str:
        params = ""
        for key, value in self.component_params_as_dict.items():
            params += f"\t{key}: {value}, \n"

        return f"componentID : {self.id} \n " \
            f"name : {self.name} \n " \
            f"parent_operation_ids : {self.parent_operation_ids} \n " \
            f"component_parameters: {{\n{params}}} \n"

    @staticmethod
    def _log_info(msg: str, logger: Optional[Logger] = None):
        if logger:
            logger.info(msg)
        else:
            print(msg)

    @staticmethod
    def _log_warning(msg: str, logger: Optional[Logger] = None):
        if logger:
            logger.warning(msg)
        else:
            print(f"WARNING: {msg}")

    @staticmethod
    def _scrub_list(dirty: Optional[List[Optional[str]]]) -> List[str]:
        """
        Clean an existing list by filtering out None and empty string values
        :param dirty: a List of values
        :return: a clean list without None or empty string values
        """
        if not dirty:
            return []
        return [clean for clean in dirty if clean]

    @staticmethod
    def is_generic_operation(operation_type) -> bool:
        return operation_type in Operation.generic_node_types


class GenericOperation(Operation):
    """
    Represents a single operation in a pipeline representing a generic (built-in) component
    """

    def __init__(self, id: str, type: str, name: str, classifier: str,
                 parent_operation_ids: Optional[List[str]] = None,
                 component_params: Optional[Dict[str, Any]] = None):
        """
        :param id:
        :param type: The type of node e.g. execution_node
        :param classifier: indicates the operation's class
        :param name: The name of the operation
        :param parent_operation_ids: List of parent operation 'ids' required to execute prior to this operation
        :param component_params: dictionary of parameter key:value pairs that are used in the creation of a
                                 a non-standard operation instance
        Component_params for "generic components" (i.e., those with one of the following classifier values:
        ["execute-notebook-node", "execute-python-node", "exeucute-r-node"]) can expect to have the following
        entries.
                filename: The relative path to the source file in the users local environment
                         to be executed e.g. path/to/file.ext
                runtime_image: The DockerHub image to be used for the operation
                               e.g. user/docker_image_name:tag
                dependencies: List of local files/directories needed for the operation to run
                             and packaged into each operation's dependency archive
                include_subdirectories: Include or Exclude subdirectories when packaging our 'dependencies'
                env_vars: List of Environmental variables to set in the docker image

                inputs: List of files to be consumed by this operation, produced by parent operation(s)
                outputs: List of files produced by this operation to be included in a child operation(s)
                cpu: number of cpus requested to run the operation
                memory: amount of memory requested to run the operation (in Gi)
                gpu: number of gpus requested to run the operation
        Entries for other (non-built-in) component types are a function of the respective component.
        """

        super().__init__(id, type, name, classifier,
                         parent_operation_ids=parent_operation_ids, component_params=component_params)

        if not component_params.get('filename'):
            raise ValueError("Invalid pipeline operation: Missing field 'operation filename'.")
        if not component_params.get('runtime_image'):
            raise ValueError("Invalid pipeline operation: Missing field 'operation runtime image'.")
        if component_params.get('cpu') and not self._validate_range(component_params.get('cpu'), min_value=1):
            raise ValueError("Invalid pipeline operation: CPU must be a positive value or None")
        if component_params.get('gpu') and not self._validate_range(component_params.get('gpu'), min_value=0):
            raise ValueError("Invalid pipeline operation: GPU must be a positive value or None")
        if component_params.get('memory') and not self._validate_range(component_params.get('memory'), min_value=1):
            raise ValueError("Invalid pipeline operation: Memory must be a positive value or None")

        # Re-build object to include default values
        self._component_params["filename"] = component_params.get('filename')
        self._component_params["runtime_image"] = component_params.get('runtime_image')
        self._component_params["dependencies"] = Operation._scrub_list(component_params.get('dependencies', []))
        self._component_params["include_subdirectories"] = component_params.get('include_subdirectories', False)
        self._component_params["env_vars"] = Operation._scrub_list(component_params.get('env_vars', []))
        self._component_params["cpu"] = component_params.get('cpu',1)
        self._component_params["gpu"] = component_params.get('gpu',1)
        self._component_params["memory"] = component_params.get('memory',256)

    @property
    def name(self) -> str:
        if self._name == os.path.basename(self.filename):
            self._name = os.path.basename(self._name).split(".")[0]
        return self._name

    @property
    def filename(self) -> str:
        return self._component_params.get('filename')

    @property
    def runtime_image(self) -> str:
        return self._component_params.get('runtime_image')

    @property
    def dependencies(self) -> Optional[List[str]]:
        return self._component_params.get('dependencies')

    @property
    def include_subdirectories(self) -> Optional[bool]:
        return self._component_params.get('include_subdirectories')

    @property
    def env_vars(self) -> Optional[List[str]]:
        return self._component_params.get('env_vars')

    @property
    def cpu(self) -> Optional[str]:
        return self._component_params.get('cpu')

    @property
    def memory(self) -> Optional[str]:
        return self._component_params.get('memory')

    @property
    def gpu(self) -> Optional[str]:
        return self._component_params.get('gpu')


    def env_vars_as_dict(self) -> Dict[str, str]:
        """
        Operation stores environment variables in a list of name=value pairs, while
        subprocess.run() requires a dictionary - so we must convert.  If no envs are
        configured on the Operation, an empty dictionary is returned, otherwise envs
        configured on the Operation are converted to dictionary entries and returned.
        """
        envs = {}
        for nv in self.env_vars:
            envs[nv["EnvironKey"]] = nv["EnvironValue"]
        return envs
    def __eq__(self, other: 'GenericOperation') -> bool:
        if isinstance(self, other.__class__):
            return super().__eq__(other)
        return False

    def _validate_range(self, value: str, min_value: int = 0, max_value: int = sys.maxsize) -> bool:
        return int(value) in range(min_value, max_value)
