import logging
import os
from logging import FileHandler
from logging import Formatter

def set_up():
    cur_dir = os.getcwd()
    log_dir = os.path.join(cur_dir, "ops_logs")
    data_dir = os.path.join(cur_dir, "data")
    query_log_dir = os.path.join(cur_dir, "query_logs")
    s3_logs = os.path.join(cur_dir,"s3_logs")

    # query_pid_dir = os.path.join(query_log_dir, "query_pid")
    # query_var_dir = os.path.join(query_log_dir, "dump_json")
    # check_upload_dir = os.path.join(query_log_dir, "check_upload")
    list_dir_name = ["log_dir",
                     "data_dir",
                     "query_log_dir",
                     "s3_logs",
                     # "query_pid_dir",
                     # "query_var_dir",
                     # "check_upload_dir"
                     ]
    dir_dict = {
        "log_dir": log_dir,
        "data_dir": data_dir,
        "query_log_dir": query_log_dir,
        "s3_logs":s3_logs,
        # "query_pid_dir": query_pid_dir,
        # "query_var_dir": query_var_dir,
        # "check_upload_dir": check_upload_dir
    }
    for name in list_dir_name:
        if not os.path.exists(dir_dict[name]):
            os.makedirs(dir_dict[name])
    return dir_dict
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(name)s %(levelname)s: %(message)s')

dir_dict = set_up()


def get_logger():

    # Create a custom logger
    logger = logging.getLogger(__name__)
    LOG_FILE = os.path.join(dir_dict.get("log_dir"), "general_message.log")
    # Create handlers
    c_handler = logging.StreamHandler()
    f_handler = logging.FileHandler(LOG_FILE)

    # c_handler.setLevel(logging.DEBUG)
    # f_handler.setLevel(logging.DEBUG)
    # Create formatters and add it to handlers
    # c_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # c_handler.setFormatter(c_format)

    f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    f_handler.setFormatter(f_format)

    # Add handlers to the logger
    logger.addHandler(c_handler)
    logger.addHandler(f_handler)

    return logger, dir_dict

def get_other_logger(name="query.logger"):
    # LOG_FORMAT = "%(asctime)s [%(levelname)s]: %(message)s in %(pathname)s:%(lineno)d"
    LOG_FORMAT = "%(asctime)s %(name)s [%(levelname)s]: %(message)s"
    LOG_LEVEL = logging.INFO

    if name == "upload.logger":
        MESSAGING_LOG_FILE = os.path.join(dir_dict.get("log_dir"), "ups_log.log")
    elif name == "check.logger":
        MESSAGING_LOG_FILE = os.path.join(dir_dict.get("log_dir"), "ack_log.log")
    elif name == "json.logger":
        MESSAGING_LOG_FILE = os.path.join(dir_dict.get("log_dir"), "jsn_log.log")
    elif name == "query.logger":
        MESSAGING_LOG_FILE = os.path.join(dir_dict.get("log_dir"), "ops_log.log")
    elif name == "insert.logger":
        MESSAGING_LOG_FILE = os.path.join(dir_dict.get("log_dir"), "ins_log.log")
    else:
        return None
    messaging_logger = logging.getLogger(name)
    messaging_logger.setLevel(LOG_LEVEL)
    messaging_logger_file_handler = FileHandler(MESSAGING_LOG_FILE)
    messaging_logger_file_handler.setLevel(LOG_LEVEL)
    messaging_logger_file_handler.setFormatter(Formatter(LOG_FORMAT))
    messaging_logger.addHandler(messaging_logger_file_handler)
    return messaging_logger
