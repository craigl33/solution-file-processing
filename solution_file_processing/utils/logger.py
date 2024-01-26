"""
Contains a custom logger class which uses some extra filters, handlers and adjustments.

The main initialization of the logger is done in the __init__.py file of the package. This way the logger is same logger
is used throughout the package and now reinitializing issues occur.

To use the logger just import it from the package:
>>> from solution_file_processing import log
>>> from . import log  # If you are in a module of the package

Then you can simply use the logger as you would use the standard python logger:
>>> log.debug('This is a debug message.')
>>> log.info('This is an info message.')
"""
import os
import datetime as dt
import logging

logging.addLevelName(logging.DEBUG, 'D')
logging.addLevelName(logging.INFO, 'I')
logging.captureWarnings(True)

# noinspection PyUnresolvedReferences,PyAttributeOutsideInit
class FilterTimeTaker(logging.Filter):
    """
    This class is a custom filter for the logging module. It calculates the time difference between the current log
    record and the last one, and adds this information to the log record.
    """

    def filter(self, record):
        """
        This method is called for each log record and modifies the record in place by adding a new attribute
        'time_relative' which contains the time difference between the current log record and the last one.

        Args:
            record (logging.LogRecord): The log record to be modified.

        Returns:
            bool: Always returns True so that the log record is not filtered out.
        """
        try:
            last = self.last
        except AttributeError:
            last = record.relativeCreated

        delta = dt.datetime.fromtimestamp(record.relativeCreated / 1000.0) - dt.datetime.fromtimestamp(
            last / 1000.0)

        duration_minutes = delta.seconds // 60  # Get the whole minutes
        duration_seconds = delta.seconds % 60  # Get the remaining seconds

        record.time_relative = '{:02d}:{:02d}'.format(duration_minutes, duration_seconds)
        self.last = record.relativeCreated

        return True


# noinspection PyAttributeOutsideInit
class CustomLogger(logging.Logger):
    """
    This class is a custom logger that extends the built-in Logger class from the logging module.
    It provides additional functionality such as changing the log file path, changing the log level,
    and enabling/disabling logging in general.
    """

    def __init__(self, name, path='logs.log', stream_level='DEBUG', file_level='INFO'):
        """
        Initializes the Logger with a name and a path for the log file.

        Args:
            name (str): The name of the logger.
            path (str, optional): The path to the log file. Defaults to 'logs.log'.
            stream_level (str or int, optional): The log level for the stream handler. Defaults to 'DEBUG'.
            file_level (str or int, optional): The log level for the file handler. Defaults to 'INFO'.
        """
        self.name = name
        super().__init__(self.name)
        self.setLevel(logging.DEBUG)

        # Create formatters
        self._fmt_stream = logging.Formatter(
            fmt="[%(asctime)s %(time_relative)5s] %(levelname)s:%(lineno)d:%(funcName)s - %(message)s",
            datefmt="%H:%M:%S")
        self._fmt_file = logging.Formatter(
            fmt="[%(asctime)s %(time_relative)5s] %(levelname)s:%(lineno)d:%(funcName)s - %(message)s",
            datefmt="%y%m%d %H:%M:%S")

        # Create stream handler
        h_stream = logging.StreamHandler()
        h_stream.setLevel(stream_level)
        h_stream.setFormatter(self._fmt_stream)
        h_stream.addFilter(FilterTimeTaker())
        self.addHandler(h_stream)

        # Create file handler
        h_file = logging.FileHandler(path, delay=True)
        h_file.setLevel(file_level)
        h_file.setFormatter(self._fmt_file)
        h_file.addFilter(FilterTimeTaker())
        self.addHandler(h_file)

        # Create pre_disabled_methods to allow disabling and enabling logging (see disable_logging and enable_logging)
        self._pre_disabled_methods = {}

    def change_log_file_path(self, new_log_file: str):
        """
        Changes the path of the log file to the given path.
        Args:
            new_log_file (str): The new path for the log file.
        """
        # Remove old file handler
        for handler in self.handlers:
            if isinstance(handler, logging.FileHandler):
                self.removeHandler(handler)
                break
        # If new_log_file is given, create a new file handler with it
        if new_log_file:
            # Create the directory if it does not exist
            os.makedirs(os.path.dirname(new_log_file), exist_ok=True)

            new_file_handler = logging.FileHandler(new_log_file)
            new_file_handler.setLevel(logging.DEBUG)
            new_file_handler.setFormatter(self._fmt_file)
            self.addHandler(new_file_handler)

    def change_log_level(self, new_log_level):
        """
        Changes the log level to the given level.

        Args:
            new_log_level (str or int): The new log level. Can be a string (e.g., 'DEBUG', 'INFO') or an integer (0-50).
        """

        level_name_to_level = {
            'CRITICAL': logging.CRITICAL,
            'ERROR': logging.ERROR,
            'WARNING': logging.WARNING,
            'INFO': logging.INFO,
            'DEBUG': logging.DEBUG,
            'NOTSET': logging.NOTSET
        }

        if isinstance(new_log_level, str):
            new_log_level = new_log_level.upper()
            if new_log_level not in level_name_to_level:
                raise ValueError(f"Invalid log level name '{new_log_level}'.")
            new_log_level = level_name_to_level[new_log_level]

        if isinstance(new_log_level, int) and (0 <= new_log_level <= 50):
            self.setLevel(new_log_level)
        else:
            raise ValueError("Invalid log level. Log level must be an integer between 0 and 50 or a valid log"
                             " level name.")

    def disable_logging(self):
        """
        Disables logging by replacing the logging methods with the standard python print function.
        """
        self._pre_disabled_methods = {
            'debug': self.debug,
            'info': self.info,
            'warning': self.warning,
            'error': self.error,
            'critical': self.critical
        }

        self.debug = print
        self.info = print
        self.warning = print
        self.error = print
        self.critical = print

    def enable_logging(self):
        """
        Enables logging by restoring the original logging methods.
        """
        try:
            self.debug = self._pre_disabled_methods['debug']
            self.info = self._pre_disabled_methods['info']
            self.warning = self._pre_disabled_methods['warning']
            self.error = self._pre_disabled_methods['error']
            self.critical = self._pre_disabled_methods['critical']
        except KeyError:
            # Ignore since it was not disabled before
            pass
