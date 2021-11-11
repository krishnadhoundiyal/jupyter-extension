"""
myextension setup
"""
import json
import sys
from pathlib import Path

import setuptools

HERE = Path(__file__).parent.resolve()

# The name of the project
name = "myextension"

lab_path = (HERE / name.replace("-", "_") / "labextension")


labext_name = "myextension"

data_files_spec = [

     ('etc/jupyter/jupyter_server_config.d', ['etc/jupyter/jupyter_server_config.d/myextension.json'])
]


# Get the package info from package.json
#pkg_json = json.loads((HERE / "package.json").read_bytes())

setup_args = dict(
    name=name,
    packages=setuptools.find_packages(),
    install_requires=[],
    zip_safe=False,
    include_package_data=False,	
     entry_points= {
        'console_scripts': [
             'jupyter-myextension = myextension.app:main'
        ]
    },
    
)

try:
    from jupyter_packaging import (
        wrap_installers,
        npm_builder,
        get_data_files
    )
    post_develop = npm_builder(
        build_cmd="install:extension", source_dir="myextension", build_dir=lab_path
    )
    setup_args["cmdclass"] = wrap_installers(post_develop=post_develop)
    setup_args["data_files"] = data_files_spec
except ImportError as e:
    import logging
    logging.basicConfig(format="%(levelname)s: %(message)s")
    logging.warning("Build tool `jupyter-packaging` is missing. Install it with pip or conda.")
    if not ("--name" in sys.argv or "--version" in sys.argv):
        raise e

if __name__ == "__main__":
    setuptools.setup(**setup_args)
