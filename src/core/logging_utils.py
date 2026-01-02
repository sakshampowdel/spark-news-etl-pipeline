import logging

class ColorFormatter(logging.Formatter):
  """
  Custom logging formatter to provide color-coded output in the terminal.

  Uses ANSI escape codes to highlight different log levels and maintains
  column alignment for scannability.
  """
  
  # ANSI Escape Codes for Colors
  cyan = "\x1b[36;20m"
  yellow = "\x1b[33;20m"
  red = "\x1b[31;20m"
  bold_red = "\x1b[31;1m"
  reset = "\x1b[0m"
  
  fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

  FORMATS = {
    logging.DEBUG: cyan + fmt + reset,
    logging.INFO: cyan + fmt + reset,
    logging.WARNING: yellow + fmt + reset,
    logging.ERROR: red + fmt + reset,
    logging.CRITICAL: bold_red + fmt + reset
  }

  def format(self, record) -> str:
    """
    Formats the log record with level-specific colors.

    Args:
      record (logging.LogRecord): The log record to be formatted.

    Returns:
      str: The colorized and formatted log string.
    """
    log_fmt = self.FORMATS.get(record.levelno)
    formatter = logging.Formatter(log_fmt, datefmt='%Y-%m-%d %H:%M:%S')
    return formatter.format(record)

def setup_logging(level=logging.INFO) -> None:
  """
  Configures the root logger with a colorized console handler.

  This should be called exactly once at the start of the application entry point
  to ensure all modules inherit the custom formatting.

  Args:
    level (int): The logging threshold (e.g., logging.INFO or logging.DEBUG).
  """
  root_logger = logging.getLogger()
  root_logger.setLevel(level)

  if not root_logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(ColorFormatter())
    root_logger.addHandler(console_handler)