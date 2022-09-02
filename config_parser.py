import os
from configparser import ConfigParser


def get_config():
    """Returns config object for SQL connection String"""
    config_object = ConfigParser()
    path = os.path.join(os.path.dirname(__file__), "config.ini")
    config_object.read(path)

    return config_object
