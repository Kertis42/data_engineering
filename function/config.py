import yaml
import os


def get_config():
    current_directory = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(current_directory,'..','config', 'config.yaml')
    with open(config_file, 'r') as yaml_file:
        return yaml.safe_load(yaml_file).get('app')


def dshop_config():
    config = get_config()
    return config.get('sources').get('dshop')

def stock_config():
    config = get_config()
    return config.get('sources').get('stock')
