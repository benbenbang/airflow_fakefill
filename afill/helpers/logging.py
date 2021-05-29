# standard library
import os
import sys

# pypi/conda library
from loguru import logger as __logger

__author__ = "benbenbang (bn@benbenbang.io)"
__license__ = "Apache 2.0"


class Formatter:
    def __init__(self, fmt, name, auto_padding=False):
        self.auto_padding = auto_padding
        self.padding = 0 if auto_padding else ""
        self.name = name
        self.fmt = fmt

    def format(self, record):
        if self.auto_padding:
            if self.name:
                length = len("{extra[name]}:{function}:{line}".format(**record))
            else:
                length = len("{name}:{function}:{line}".format(**record))
            self.padding = max(self.padding, length)
            record["extra"]["padding"] = " " * (self.padding - length)
        return self.fmt


def check_env(env):
    debug = os.getenv(env, "")
    return f"{debug}".lower() == "true"


def getLogger(
    name: str = None,
    debug: bool = False,
    diagnose: bool = False,
    fmt: str = None,
    auto_padding: bool = False,
    enqueue: bool = False,
    *,
    logger_=__logger,
    formatter_=Formatter,
    **kwargs,
):
    """ logger = getLogger() -> Production ready
    Logger stream to std our / err, can be easily parsed by K8s, Kibana...etc

    Level name  Severity value  Logger method
    TRACE       5               logger.trace()
    DEBUG       10              logger.debug()
    INFO        20              logger.info()
    SUCCESS     25              logger.success()
    WARNING     30              logger.warning()
    ERROR       40              logger.error()
    CRITICAL    50              logger.critical()

    Examples:

        Plug and play:
            logger = getLogger()

        For debugging:
            logger = getLogger(debug=True)

            You can also enable the diagnose to see what was wrong:
            logger = getLogger(debug=True, diagnose=True)

        To collect all the same module together, you can put, for example, the module name:
            # In module_a.file_a
            logger = getLogger("module_a")
            logger.info("hi 123")

            # In module_a.file_b
            logger = getLogger("module_a")
            logger.info("hi 456")
            ----
            └ This will render the output to be:
                2020-11-28 at 00:00:00 | INFO     | module_a:<module>:7 - hi 123
                2020-11-28 at 00:00:00 | INFO     | module_a:<module>:7 - hi 456

            # In module_b.file_a
            logger = getLogger("module_b.file_a")
            logger.info("hi 789")
            ----
            └ This will render the output to be:
                2020-11-28 at 00:00:00 | INFO     | module_b.file_a:<module>:7 - hi 789
    """
    # Settings and inject from env variables
    if check_env("DEBUG"):
        debug = True

    if os.getenv("LOGGING_DIAGNOSE"):
        diagnose = True

    if os.getenv("LOGGING_FMT"):
        fmt = os.environ["LOGGING_FMT"]

    if os.getenv("LOGGING_AUTO_PADDING"):
        auto_padding = True

    fmt = (
        fmt
        or "<green>{time:YYYY-MM-DD at HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{extra[name]}</cyan>:<cyan>{function}</cyan>:<cyan>{line}{extra[padding]}</cyan> - <level>{message}</level>\n{exception}"
        % {"name": name}
        if name
        else "<green>{time:YYYY-MM-DD at HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}{extra[padding]}</cyan> - <level>{message}</level>\n{exception}"
    )

    # Remove default settings
    logger_.remove()

    # Debug level settings
    DEBUG_LEVEL = "DEBUG" if debug else "INFO"
    DEBUG_FILTER = None if debug else lambda record: record["level"].no <= 30

    # Patch name if provided`
    patch = {"name": name, "padding": ""} if name else {"padding": ""}
    logger_ = logger_.patch(lambda record: record["extra"].update(patch))

    # Init Formatter
    Formatter = formatter_(fmt=fmt, name=name, auto_padding=auto_padding)

    # Add handlers for stdout / stderr
    logger_.add(
        sys.stdout, level=DEBUG_LEVEL, filter=DEBUG_FILTER, diagnose=diagnose, format=Formatter.format, enqueue=enqueue
    )
    logger_.add(
        sys.stderr,
        level="ERROR",
        filter=lambda record: record["level"].no >= 40,
        diagnose=diagnose,
        format=Formatter.format,
        enqueue=enqueue,
    )

    return logger_
