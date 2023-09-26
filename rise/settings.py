"""
This module contains the package-wide settings.
"""
import toml
import os

from .utils.logger import log


class _Settings:
    """
    This class contains the package-wide settings. It should not be instantiated and only used via the settings
    variable (see below).
    """

    # noinspection PyMissingOrEmptyDocstring

    _config_name = None
    _cfg = {}
    _logging = True

    _description = {
        'config_name': 'Name of the configuration file to use. Needs to be set before anything is done.',
        'cfg': 'Dictionary containing the settings from the configuration file.',
        'logging': 'Whether to use logging or simple print statements.'
    }

    @property
    def config_name(self):
        return self._config_name

    @config_name.setter
    def config_name(self, value):
        self._config_name = value
        with open(os.path.join('configurations', value), 'r') as f:
            self.cfg = toml.load(f)
        log.change_log_file_path(os.path.join('logs', self._config_name.replace('.toml', '.log')))

    @property
    def cfg(self):
        if not self._cfg:
            raise Exception('No configuration loaded. Please set the config_name property first.')
        return self._cfg

    @cfg.setter
    def cfg(self, value):
        self._cfg = value

    @property
    def logging(self):
        return self._logging

    @logging.setter
    def logging(self, value):
        self._logging = value
        if value:
            log.enable_logging()
        else:
            log.disable_logging()

    def describe_settings(self):
        """Prints (not logs) all available settings and their description."""
        print('Available settings:')
        for key, value in self._description.items():
            print(f' \t- {key}: {value}')
            print("")
        print('Available methods:\n'
              '\t- describe_settings: Prints (not logs) all available settings and their description.\n'
              '\t- get_current_settings: Returns a dictionary with all current settings.')

    def get_current_settings(self):
        """Returns a dictionary with all current settings."""
        return {key: value for key, value in self.__dict__.items() if not key.startswith('_')}


settings = _Settings()
