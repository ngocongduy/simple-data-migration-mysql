import mysql.connector
from simple_logger import get_other_logger, get_logger
import os
import pandas as pd
from pandas import DataFrame
from datetime import datetime, timedelta
import sys, getopt

logger, dir_dict = get_logger()

def query(config_values=None, query_tring=None, *args):
    def _try_query(query_tring, *args):
        # config params
        if config_values is None:
            logger.info("No config values!")
            return
        # Connect
        cnx = mysql.connector.connect(**config_values)
        # For query
        cursor = cnx.cursor()
        # cursor = cnx.cursor(buffered=True)
        rows, field_names = None, None
        try:
            cursor.execute(query_tring, args)
            rows = cursor.fetchall()

            field_names = [i[0] for i in cursor.description]
            rows = [list(r) for r in rows]
            # logger.info(len(rows))
        except Exception as e:
            logger.info(f"Catch exception: {e}")
            logger.debug("Catch exception: {}".format(e))
        cnx.close()
        logger.info("Finished Query!")
        return rows, field_names

    if query_tring is None:
        logger.info("Query string cannot be none!")
        return None, None
    else:
        logger.info("Custom query!")
        try:
            return _try_query(query_tring, *args)
        except:
            return None, None
try:
    from env import PROD_DB_CREDENTIAL
except:
    PROD_DB_CREDENTIAL = {

    }


def generate_log_file_name(start_day, end_day):
    file_name = "{}_{}.csv".format(start_day, end_day)
    return file_name


def generate_log_file_path(file_name, file_path):
    path_to_file = os.path.join(file_path, file_name)
    return path_to_file


def generate_data_file_path(batch_log_file_name, loan_app_id, process_instance_id, execution_id, file_path):
    file_name = "{}_{}_{}_{}.json".format(batch_log_file_name, loan_app_id, process_instance_id, execution_id)
    path_to_file = os.path.join(file_path, file_name)
    return path_to_file


def query_get_pid_and_save_csv(start_day, end_day, path_to_file):
    query_string = (
        'select l.loan_application_id, l.process_instance_id, r.ID_, l.user_id, r.START_USER_ID_, r.BUSINESS_KEY_ from Workflow.ACT_RU_EXECUTION r '
        'left join Workflow.los_user_loan_data l on r.PROC_INST_ID_ = l.process_instance_id '
        "where l.application_stage != 'DISBURSED' and r.BUSINESS_KEY_ is not null "
        "and l.modified >= %s and l.modified < %s ;"
    )

    rows, col_names = query(PROD_DB_CREDENTIAL, query_string, start_day, end_day)
    num_of_row = len(rows)

    logger.info("Number of row return from loan table {}".format(num_of_row))
    dic_data = dict()
    for i in range(len(col_names)):
        dic_data[col_names[i]] = []

    dic_data["is_get_vars"] = []
    dic_data["name_vars_file"] = []

    if num_of_row > 0:
        cols = list(zip(*rows))

        for i in range(len(col_names)):
            dic_data[col_names[i]] = cols[i]

        # Add a column to check for updated
        additional_col_1 = ['waiting_to_get_vars' for _ in range(num_of_row)]
        additional_col_2 = ['waiting_to_send_to_s3' for _ in range(num_of_row)]

        dic_data["is_get_vars"] = additional_col_1
        dic_data["name_vars_file"] = additional_col_2

        del rows

    else:
        logger.info("No rows return!")

    # Save file
    df = DataFrame(dic_data)
    logger.info("To save query log: {}".format(path_to_file))
    df.to_csv(path_to_file)


def query_get_vars_to_json(process_instance_id, execution_id, loan_app_id, batch_log_file_name, start_day, end_day):
    query_string = (
        'select * from Workflow.ACT_RU_VARIABLE '
        "where PROC_INST_ID_ = %s ;"
        # "where EXECUTION_ID_ = %s ;"

    )
    rows, col_names = query(PROD_DB_CREDENTIAL, query_string, str(process_instance_id))
    num_of_row = len(rows)

    logger.info("Number of row return from variable table exec id: {} - {} ".format(process_instance_id, num_of_row))
    if num_of_row > 0:
        cols = list(zip(*rows))
        dic_data = dict()
        for i in range(len(col_names)):
            dic_data[col_names[i]] = cols[i]
        df = DataFrame(dic_data)

        # Each period will be put in a folder /data/startDay_endDay
        data_file_path = os.path.join(os.getcwd(), 'data')
        directory = os.path.join(data_file_path, "{}_{}".format(start_day, end_day))
        if not os.path.exists(directory):
            os.makedirs(directory)
        data_file_path = directory
        path_to_file = generate_data_file_path(batch_log_file_name, loan_app_id, process_instance_id, execution_id,
                                               data_file_path)

        # df.to_json(path_to_file, force_ascii=False, orient="table", index=False) # Table-like json format
        df.to_json(path_to_file, force_ascii=False, orient="split", index=False)  # nested array data
        # df.to_json(path_to_file, force_ascii=False)
        return path_to_file
    else:
        return None


def read_csv_and_query_vars(start_day: str, end_day: str, file_path: str):
    file_name = generate_log_file_name(start_day, end_day)
    path_to_log_file = generate_log_file_path(file_name, file_path)

    logger.info("Read query_log file ... {}".format(path_to_log_file))
    df = pd.read_csv(path_to_log_file, index_col=0)
    logger.info("Read query_log file successfully")

    is_updated = False
    num_of_rows = len(df)
    for i in range(num_of_rows):
        logger.info("Process item number {}/{} in the batch".format(i + 1, num_of_rows))
        # Make sure that these id are integer

        loan_app_id = int(df['loan_application_id'][i])
        loan_pid = int(df['process_instance_id'][i])
        exec_id = int(df['ID_'][i])

        try:
            # Additional check in case of retrieve on going job
            if df['is_get_vars'][i] in ['notFound', 'done']:
                logger.info("This record with execution id {} might be dumped in json already.".format(exec_id))
                continue

            file = query_get_vars_to_json(process_instance_id=loan_pid, execution_id=exec_id, loan_app_id=loan_app_id,
                                          batch_log_file_name=file_name, start_day=start_day, end_day=end_day)
            if file is None:
                logger.info("No variables for the execution {}".format(exec_id))
                df['is_get_vars'][i] = 'notFound'
                df['name_vars_file'][i] = 'done'
            elif os.path.exists(file):
                logger.info("Checked existed! {}".format(file))
                df['is_get_vars'][i] = 'done'
            is_updated = True
        except Exception as e:
            logger.info("Facing error for execution id".format(exec_id))
            logger.debug("Exception {} ".format(e))
            return None

    if is_updated:
        df.to_csv(path_to_log_file, mode='w')
        return True
    else:
        logger.info("May be no loans in {}".format(path_to_log_file))
        return False


def retrieve_ongoing_job_period(log_file_path):
    names = ["col{}".format(i) for i in range(1, 14)]
    df = pd.read_csv(log_file_path, delimiter=' ', index_col=False, names=names)
    period_col = "col10"
    status_col = "col5"
    last_line = len(df) - 1
    if len(df) > 0:
        if not df[period_col][last_line] == df[period_col][last_line - 1]:
            return None

        if not (df[status_col][last_line] == "Finish" and df[status_col][last_line - 1] == "Start"):
            return None
        else:
            return df[period_col][last_line]
    else:
        return ""


def read_log_and_check_upload(s3_log_file_path: str, batch_log_file_name: str, query_log_dir: str):
    batch_log_file_path = generate_log_file_path(batch_log_file_name, query_log_dir)
    # Read line by line and handle
    df = pd.read_csv(s3_log_file_path, delimiter='/r', names=["value"], engine="python")
    query_log_df = pd.read_csv(batch_log_file_path, index_col=0)
    num_rows = len(df)

    file_list = []
    for i in range(num_rows):
        line = str(df["value"][i])
        if not line.startswith("upload:"):
            continue
        parts = line.split()
        file_name = parts[-1]
        if file_name.startswith("s3://"):
            file_list.append(file_name)

    is_found = False
    for k in range(len(query_log_df)):
        loan_app_id = int(query_log_df['loan_application_id'][k])
        loan_pid = int(query_log_df['process_instance_id'][k])
        exec_id = int(query_log_df['ID_'][k])
        s3_bucket_name = "s3://my-test-upload-file-bucket"
        generated_file_name = generate_data_file_path(batch_log_file_name, loan_app_id, loan_pid, exec_id,
                                                      s3_bucket_name)
        if generated_file_name in file_list:
            logger.info("Found {} in s3 upload log.".format(generated_file_name))
            query_log_df['name_vars_file'][k] = generated_file_name
            is_found = True

    if is_found:
        logger.info("Update query log file {}.".format(batch_log_file_path))
        query_log_df.to_csv(batch_log_file_path, mode='w')
    return is_found


def check_args(argv):
    # Arguments checking
    start_day_as_string = ''
    check_ongoing_job = ''
    early_stop_day_as_string = ''
    job_name = ''
    day_format = "%Y-%m-%d"

    try:
        opts, args = getopt.getopt(argv, "hs:c:e:j:", ["start=", "check=", "early-stop", "job"])
    except getopt.GetoptError:
        logger.info('test.py -s <start> -c <check> -e <early-stop> -j <job>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            logger.info('test.py -s <start> -c <check> -e <early-stop> -j <job>')
            sys.exit()
        elif opt in ("-s", "--start"):
            start_day_as_string = arg
        elif opt in ("-c", "--check"):
            check_ongoing_job = arg
        elif opt in ("-e", "--early-stop"):
            early_stop_day_as_string = arg
        elif opt in ("-j", "--job"):
            job_name = arg
    job_list = ["query_pid", "query_var_to_json", "upload", "check_upload"]
    if job_name not in job_list:
        logger.info("Allowed job to run : " + str(job_list))
        sys.exit(2)

    try:
        to_run_day = datetime.fromisoformat(start_day_as_string)
        lower_day_limit = datetime(2018, 1, 1)
        upper_day_limit = datetime(2020, 1, 1)

        if len(early_stop_day_as_string) > 0:
            try:
                early_stop = datetime.fromisoformat(early_stop_day_as_string)
                if not (lower_day_limit <= early_stop < upper_day_limit):
                    logger.info(
                        "Early stop day should satisfy: {} <= early_stop < {} ".format(
                            lower_day_limit.strftime(day_format),
                            upper_day_limit.strftime(
                                day_format)))
                    sys.exit(2)
            except:
                logger.info("Please pass a day like 'yyyy-dd-mm' for early stop !")
                sys.exit(2)
            upper_day_limit = early_stop

        if not (lower_day_limit <= to_run_day < upper_day_limit):
            logger.info("Start day should satisfy: {} <= start < {} ".format(lower_day_limit.strftime(day_format),
                                                                             upper_day_limit.strftime(day_format)))
            sys.exit(2)
    except:
        logger.info("Please pass a day like 'yyyy-dd-mm' !")
        sys.exit(2)

    logger.info('Start day to run {}'.format(start_day_as_string))
    logger.info("To check on going job {}".format(check_ongoing_job))
    logger.info("Early stop when reach day {}".format(early_stop_day_as_string))
    logger.info("Job name to run {}".format(job_name))
    return to_run_day, check_ongoing_job, upper_day_limit, job_name


def main(argv):

    to_run_day, check_ongoing_job, upper_day_limit, job_name = check_args(argv)
    step = 1
    day_format = "%Y-%m-%d"

    # Business logic start from here
    messaging_logger = get_other_logger()
    json_logger = get_other_logger("json.logger")
    uploading_logger = get_other_logger("upload.logger")
    checking_logger = get_other_logger("check.logger")

    start_date = to_run_day
    # cur_dir = os.getcwd()
    # job_log_dir = "ops_logs"

    log_dir = dir_dict.get("log_dir")

    # Get the last job in ops_logs
    # Set the start_date = <last_day> operation
    if len(check_ongoing_job) > 0 and check_ongoing_job == "yes":
        # job_list = ["query_pid", "query_var_to_json", "upload", "check_upload"]
        if job_name == "query_pid":
            log_file_path = os.path.join(log_dir, "ops_log.log")
        elif job_name == "query_var_to_json":
            log_file_path = os.path.join(log_dir, "jsn_log.log")
        elif job_name == "upload":
            log_file_path = os.path.join(log_dir, "ups_log.log")
        elif job_name == "check_upload":
            log_file_path = os.path.join(log_dir, "ack_log.log")

        period = retrieve_ongoing_job_period(log_file_path)
        if period is None:
            logger.info("Some thing went wrong while trying to retrieve ongoing job, manual review please!")
            sys.exit(2)
        elif not len(period) > 0:
            logger.info("Log for corresponding job {} is empty".format(job_name))
        else:
            try:
                expected_a_date_string = period.split('_')[-1]
                start_date = datetime.fromisoformat(expected_a_date_string)
            except:
                logger.info(
                    "Some thing went wrong while trying to retrieve last date of the ongoing job, manual review please!")
                sys.exit(2)
    end_date = start_date + timedelta(days=step)
    # Check for exceeding upper limit
    if end_date >= upper_day_limit:
        end_date = upper_day_limit

    start_date_string = start_date.strftime(day_format)
    end_date_string = end_date.strftime(day_format)
    query_log_dir = dir_dict.get("query_log_dir")
    # query_log_file_path = os.path.join(cur_dir, 'query_logs')
    if job_name == "query_pid":

        file_name = generate_log_file_name(start_date_string, end_date_string)
        path_to_file = generate_log_file_path(file_name, query_log_dir)

        # Additional check in case of retrieve on going job: do not query and re-write the file
        if os.path.exists(path_to_file):
            logger.info("The file already exist {}, skipped".format(path_to_file))
            return

        messaging_logger.info(
            "Start the job for period: {}_{} with step: {}".format(start_date_string, end_date_string, step))
        logger.info("Start the job for start-end: {}_{} with step: {}".format(start_date_string, end_date_string, step))

        try:
            query_get_pid_and_save_csv(start_date_string, end_date_string, path_to_file)
        except Exception as e:
            logger.info(
                "Exception happened for period {}_{} with step: {}".format(start_date_string, end_date_string, step))
            logger.debug("Exception {}".format(e))
            messaging_logger.info(
                "Fail the job for period: {}_{} with step: {}".format(start_date_string, end_date_string, step))
            sys.exit(1)

        logger.info(
            "Finish the job for start-end: {}_{} with step: {}".format(start_date_string, end_date_string, step))
        messaging_logger.info(
            "Finish the job for period: {}_{} with step: {}".format(start_date_string, end_date_string, step))

    elif job_name == "query_var_to_json":
        # Check if the log file exist or not
        file_name = generate_log_file_name(start_date_string, end_date_string)
        path_to_file = generate_log_file_path(file_name, query_log_dir)

        if not os.path.exists(path_to_file):
            logger.info("The log file does not exist {}, terminated.".format(path_to_file))
            return

        json_logger.info(
            "Start the job for period: {}_{} with step: {}".format(start_date_string, end_date_string, step))

        logger.info("Start the job for start-end: {}_{} with step: {}".format(start_date_string, end_date_string, step))
        try:
            is_generate_success = read_csv_and_query_vars(start_date_string, end_date_string, query_log_dir)
            if is_generate_success is None:
                logger.info(
                    "To check for retry for batch in the period {}_{}".format(start_date_string, end_date_string))
                raise Exception("Something went wrong is_generate_success=None ")
            elif is_generate_success:
                logger.info("Generate files successfully.")
            else:
                logger.info("There is no loan in the period {}_{}".format(start_date_string, end_date_string))
        except Exception as e:
            logger.info(
                "Exception happened for period {}_{} with step: {}".format(start_date_string, end_date_string, step))
            logger.debug("Exception {}".format(e))
            json_logger.info(
                "Fail the job for period: {}_{} with step: {}".format(start_date_string, end_date_string, step))
            sys.exit(1)

        logger.info(
            "Finish the job for start-end: {}_{} with step: {}".format(start_date_string, end_date_string, step))
        json_logger.info(
            "Finish the job for period: {}_{} with step: {}".format(start_date_string, end_date_string, step))

    elif job_name == "upload":
        uploading_logger.info(
            "Start the upload-job for period: {}_{} with step: {}".format(start_date_string, end_date_string, step))
        # Upload file and archived:
        params = {"start": start_date_string, "end": end_date_string}
        # "aws s3 mv ./data/{start}_{end} s3://my-test-upload-file-bucket >> ./s3_logs/s3_{start}_{end}.log 2>> ./err.log"
        # "aws s3 sync ./data/{start}_{end} s3://my-test-upload-file-bucket >> ./s3_logs/s3_{start}_{end}.log 2>> ./err.log"
        # s3_cli_cmd = "aws s3 cp ./data/{start}_{end} s3://my-test-upload-file-bucket >> ./s3_logs/s3_{start}_{end}.log 2>> ./err.log".format(
        #     **params)
        s3_cli_cmd = "aws s3 cp --recursive ./data/{start}_{end}/ s3://my-test-upload-file-bucket >> ./s3_logs/s3_{start}_{end}.log 2>&1 ".format(
            **params)

        json_file_dir = "./data/{start}_{end}".format(**params)
        is_generate_success = os.path.exists(json_file_dir)

        if is_generate_success:
            try:
                logger.info("Execute upload command ..." + s3_cli_cmd)
                os.system(s3_cli_cmd)
            except Exception as e:
                logger.info("Exception happened during execute AWS CLI to upload.")
                logger.debug("Exception {}".format(e))
                uploading_logger.info(
                    "Fail the upload-job for period: {}_{} with step: {}".format(start_date_string, end_date_string,
                                                                                 step))
                sys.exit(1)
            logger.info("Executed upload command")
        else:
            logger.info("Did not execute upload command due to no files generated!")
        uploading_logger.info(
            "Finish the upload-job for period: {}_{} with step: {}".format(start_date_string, end_date_string, step))

    elif job_name == "check_upload":
        checking_logger.info(
            "Start the check-job for period: {}_{} with step: {}".format(start_date_string, end_date_string, step))
        logger.info("Start checking uploaded files and update log")

        params = {"start": start_date_string, "end": end_date_string}
        s3_log_file_name = "s3_{start}_{end}.log".format(**params)

        s3_log_file_path = os.path.join(dir_dict.get("s3_logs"), s3_log_file_name)

        batch_log_file_name = generate_log_file_name(start_date_string, end_date_string)
        is_update_success = os.path.exists(s3_log_file_path)
        if is_update_success:
            is_log_updated = None
            try:
                is_log_updated = read_log_and_check_upload(s3_log_file_path, batch_log_file_name, query_log_dir)
            except Exception as e:
                logger.info("Exception happened during checking ...")
                logger.debug("Exception {}".format(e))
            if is_log_updated is None:
                logger.info("Something went wrong, please do a manual check.")
                checking_logger.info(
                    "Fail the upload-job for period: {}_{} with step: {}".format(start_date_string, end_date_string,
                                                                                 step))
                sys.exit(1)
            elif is_log_updated:
                logger.info("Log file was updated.")
            else:
                logger.info("Log file was not updated. Maybe no record!")
        else:
            logger.info("Did not execute upload command due to no files generated!")

        checking_logger.info(
            "Finish the check-job for period: {}_{} with step: {}".format(start_date_string, end_date_string, step))


if __name__ == "__main__":
    main(sys.argv[1:])
