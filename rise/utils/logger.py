"""
Contains a custom logger class which uses some extra filters and handlers.
"""
import datetime as dt
import logging


# noinspection PyUnresolvedReferences,PyAttributeOutsideInit
class FilterTimeTaker(logging.Filter):
    """Filter that takes the time since the last log message. This can be used as 'time_relative' in the log format."""

    def filter(self, record):
        """Overwrites the filter method to take the time since the last log message."""
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

logging.addLevelName(logging.DEBUG, 'D')
logging.addLevelName(logging.INFO, 'I')

log = logging.getLogger('rise')
log.setLevel(logging.DEBUG)

# Create formatters
fmt_stream = logging.Formatter(
    fmt="[%(asctime)s %(time_relative)5s] %(levelname)s:%(lineno)d:%(funcName)s - %(message)s",
    datefmt="%H:%M:%S")
fmt_file = logging.Formatter(
    fmt="[%(asctime)s %(time_relative)5s] %(levelname)s:%(lineno)d:%(funcName)s - %(message)s",
    datefmt="%y%m%d %H:%M:%S")

# Create stream handler
h_stream = logging.StreamHandler()
h_stream.setLevel(logging.DEBUG)
h_stream.setFormatter(fmt_stream)
h_stream.addFilter(FilterTimeTaker())
log.addHandler(h_stream)

# Create file handler
h_file = logging.FileHandler('logs.log', delay=True)
h_file.setLevel(logging.DEBUG)
h_file.setFormatter(fmt_file)
h_file.addFilter(FilterTimeTaker())
log.addHandler(h_file)

def change_log_file_path(logger: logging.Logger, new_log_file: str):
    """
    Changes the log file path of the given logger. If the logger does not have a file handler, a new one is created.

    Args:
        logger (logging.Logger): Logger to change the log file path of.
        new_log_file (str): New log file path.
    """
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler):
            logger.removeHandler(handler)
            break
    if new_log_file:
        new_file_handler = logging.FileHandler(new_log_file)
        new_file_handler.setLevel(logging.DEBUG)
        new_file_handler.setFormatter(fmt)
        logger.addHandler(new_file_handler)

def change_log_level(logger: logging.Logger, new_log_level):
    """
    Changes the log level of the given logger. If the logger does not have a level set, the new level will be applied.
    The new_log_level can be provided as an integer or a string representing the log level name.

    Args:
        logger (logging.Logger): Logger to change the log level of.
        new_log_level: New log level (int or str).
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
        logger.setLevel(new_log_level)
    else:
        raise ValueError("Invalid log level. Log level must be an integer between 0 and 50 or a valid log level name.")
