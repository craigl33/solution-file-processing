"""
This module contains the package-wide settings.
"""
import toml
import os

class _Settings:
    """
    This class contains the package-wide settings. It should not be instantiated and only used via the settings
    variable (see below).
    """

    # noinspection PyMissingOrEmptyDocstring

    _config_name = None
    _cfg = {}

    @property
    def config_name(self):
        return self._config_name

    @config_name.setter
    def config_name(self, value):
        self._config_name = value
        with open(os.path.join('configurations', value), 'r') as f:
            self.cfg = toml.load(f)

    @property
    def cfg(self):
        if not self._cfg:
            raise Exception('No configuration loaded. Please set the config_name property first.')
        return self._cfg

    @cfg.setter
    def cfg(self, value):
        self._cfg = value

settings = _Settings()
