import os
import yaml


def get_config(config_path=None):
    """Get configuration from .yaml file as dict.
    """
    if not config_path:
        config_path = os.path.join(os.path.dirname(__file__), "..", "config.yaml")
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    return config
