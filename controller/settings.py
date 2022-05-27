import logging
import yaml
import logging.config
from enum import Enum
from controller.file_operation import read_yml


class TestsuiteSettings:
    def __init__(self):
        self.url_prefix = ""
        self.api_version = Settings.default_api_version


class EnumTestEnvironment(str, Enum):
    STAGING = "STAGING"
    PROD = "PRODUCTION"


class Settings:
    DEFAULT_API_VERSION = 'v1'
    DEFAULT_SETTINGS_CONFIG = './config/setup.yml'

    def _init_variables(self):
        self.url_prefix = self.setup_config[self.environment.value]["URL_PREFIX"]

    # ----------------------------------------------------------------------------#
    # initialize logging when doing test base setup
    # ----------------------------------------------------------------------------#
    def __init__(self, request) -> None:
        # get custom config from command line args
        args = {
            'env': request.config.getoption('--env', default=None),
            'dataset': request.config.getoption('--dataset', default=None),
            'api_version': request.config.getoption('--api_version', default=None),
            'settings_file': request.config.getoption('--settings_file', default=None)
        }
        logging.info('input args: %s', args)

        self.set_env(args['env'])
        self.dataset = self.set_dataset(args["dataset"])
        self.api_version = args["api_version"] if args["api_version"] else Settings.DEFAULT_API_VERSION
        # get settings from config file
        if args['settings_file']:
            self.DEFAULT_SETTINGS_CONFIG = args['settings_file']

        with open(self.DEFAULT_SETTINGS_CONFIG, "r") as config:
            self.setup_config = yaml.load(config, Loader=yaml.FullLoader)

        self._init_variables()

    def set_env(self, env: str):
        if env is not None:
            self.environment = env.upper()
            logging.info('testing env is set from args: %s', self.environment)
        else:
            self.environment = str(self.setup_config["Environment"]).upper()
            logging.info('testing env is set from setup.cfg: %s', self.environment)

        for enum_env in EnumTestEnvironment:
            if self.environment == enum_env.value:
                self.environment = enum_env
                break
        if not self.environment:
            self.environment = EnumTestEnvironment.STAGING

    def set_dataset(self, data_name: str):
        if data_name is not None:
            data = read_yml(f'./testdata/{data_name}.yml')
        else:
            data = None
        return data
