# scripts/utils.py

import logging
import os
import pandas as pd


def setup_logger(log_file_path="../logs/pipeline.log"):
    """Logger setup...
    This creates the log directory if needed, writes INFO+ messages to a
    log file, and also prints logs to the console. It is easy to understand
    and safe to call multiple times (it clears existing handlers first).
    """
    try:
        log_dir = os.path.dirname(os.path.abspath(log_file_path))
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
    except Exception as e:
        print('Could not create log directory:', e)

    # Clear existing handlers so repeated calls don't duplicate messages
    root = logging.getLogger()
    if root.handlers:
        root.handlers = []

    # Configure logging to write to file
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        filename=log_file_path,
        filemode="a",
    )

    # Also print logs to console for quick feedback
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    console.setFormatter(formatter)
    root.addHandler(console)

    logging.info('Logger initialized.')


def save_output(df, output_path):
    """Save a DataFrame to CSV and show a simple message.

    For prints a message and also writes an INFO log entry.
    """
    try:
        df.to_csv(output_path, index=False)
        print('Output saved to', output_path)
        logging.info('Output saved successfully: %s', output_path)
    except Exception as e:
        print('Error saving output:', e)
        logging.error('Error saving output: %s', e)


def log_message(message, level='info'):
    """Simple wrapper to log or print a message at a chosen level.

    This prints the message to console as well as logging it.
    """
    if level == 'error':
        logging.error(message)
        print('ERROR:', message)
    elif level == 'warning':
        logging.warning(message)
        print('WARNING:', message)
    else:
        logging.info(message)
        print('INFO:', message)

