import getopt
import pandas as pd
import os
import mysql.connector
import sys
from datetime import datetime, timedelta

try:
    from env import UAT_DB_CREDENTIAL

    PROD_DB_CREDENTIAL = UAT_DB_CREDENTIAL
except:
    PROD_DB_CREDENTIAL = {

    }

from simple_logger import get_logger, get_other_logger

logger, dir_dict = get_logger()
logger.info(dir_dict)


def insert_bulk(config_values=None, query_string=None, data=None):
    def _try_query(query_string, data):
        # config params
        if config_values is None:
            logger.info("No config values!")
            return None

        cnx = mysql.connector.connect(**config_values)
        cursor = cnx.cursor()
        is_successful = True
        try:
            cursor.executemany(query_string, data)
            cnx.commit()
            logger.info("Insert finished! Row count {}".format(cursor.rowcount))
        except mysql.connector.IntegrityError as err:
            logger.info("Violate constraints!")
            logger.debug(err)
            is_successful = False
            # Should log into a file
        except mysql.connector.DatabaseError as err:
            logger.info("Other database error, retry!")
            logger.debug(err)
            # Should log into a file to retry later
            is_successful = None
        except Exception as e:
            logger.info("Unhandled error happened!")
            logger.debug(e)
            is_successful = None
        finally:
            cursor.close()
            return is_successful

    if query_string is None:
        logger.info("Query string cannot be none!")
        return None
    elif type(data) is not list:
        logger.info("Data should be a list of item-ordered tuple")
    else:
        logger.info("Try to insert... !")
        try:
            is_successful = _try_query(query_string, data)
            return is_successful
        except Exception as e:
            logger.info("Outer most exception catching: " + str(e))
            return None


def check_args(argv):
    # Arguments checking
    start_day_as_string = ''
    end_day_as_string = ''
    period = ''

    try:
        opts, args = getopt.getopt(argv, "hs:e:p:", ["start=", "end=", "period="])
    except getopt.GetoptError:
        logger.info('test.py -s <start> -e <end> -p <period>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            logger.info('test.py -s <start> -e <end> -p <period>')
            sys.exit()
        elif opt in ("-s", "--start"):
            start_day_as_string = arg
        elif opt in ("-e", "--end"):
            end_day_as_string = arg
        elif opt in ("-p", "--period"):
            period = arg

    if not (len(start_day_as_string) > 0 and len(end_day_as_string) > 0 and len(period) > 0):
        logger.info("Please pass start day, end day with format 'yyyy-dd-mm' and an integer for period ")
        sys.exit(2)
    try:
        start = datetime.fromisoformat(start_day_as_string)
        end = datetime.fromisoformat(end_day_as_string)
        if start >= end:
            logger.info("Start day must lower than end day with format 'yyyy-dd-mm' !")
            sys.exit(2)
    except:
        logger.info("Please pass a day like 'yyyy-dd-mm' !")
        sys.exit(2)
    try:
        period = int(period)
        if not (period > 0):
            logger.info("Period must be an integer and larger than 0")
            sys.exit(2)
    except:
        logger.info("Please pass an integer for period !")
    logger.info('Start day to run {}'.format(start_day_as_string))
    logger.info("End day to run {}".format(end_day_as_string))
    logger.info("Period / step for each iterate {}".format(period))
    return start, end, period


def read_csv_query_log(start_day_string, end_day_string, dir_dict):
    query_logs_dir = dir_dict.get("query_log_dir")
    logger.info(query_logs_dir)
    log_file_name = "{}_{}.csv".format(start_day_string, end_day_string)
    log_file_path = os.path.join(query_logs_dir, log_file_name)
    logger.info(log_file_path)
    df = pd.read_csv(log_file_path, index_col=0)
    return df


def bulk_insert_for_a_batch(df):
    insert_query = ("INSERT INTO Workflow.s3_migration_ru_vars "
                    "(loan_application_id,process_instance_id,execution_id,user_id,START_USER_ID_,BUSINESS_KEY_, name_vars_file) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s)"
                    )

    data = []
    no_of_row = len(df)
    for i in range(no_of_row):
        loan_id = str(int(df["loan_application_id"][i]))
        loan_pid = str(int(df["process_instance_id"][i]))
        exec_id = str(int(df["ID_"][i]))
        user_id = str(int(df["user_id"][i]))
        start_user_id = str(df["START_USER_ID_"][i])
        business_key = str(int(df["BUSINESS_KEY_"][i]))
        name_vars_file = str(df["name_vars_file"][i])
        tup = (loan_id, loan_pid, exec_id, user_id, start_user_id, business_key, name_vars_file)
        data.append(tup)
    logger.info(no_of_row)
    logger.info(len(data))
    for ele in data:
        logger.info(ele)
    logger.info(PROD_DB_CREDENTIAL)
    return insert_bulk(PROD_DB_CREDENTIAL, insert_query, data)



def main(argv):
    day_format = "%Y-%m-%d"
    start, end, period = check_args(argv)
    insert_logger = get_other_logger("insert.logger")

    upper_day_limit = datetime(2020, 1, 1)

    if end < upper_day_limit:
        upper_day_limit = end

    to_run_day = start

    while to_run_day < upper_day_limit:
        start_day_string = to_run_day.strftime(day_format)

        to_run_day = to_run_day + timedelta(days=period)
        if to_run_day > upper_day_limit:
            to_run_day = upper_day_limit

        end_day_string = to_run_day.strftime(day_format)
        logger.info("Start to read for the batch: {}_{}".format(start_day_string, end_day_string))
        try:
            insert_logger.info(
                "Start the job for period: {}_{} with step: {}".format(start_day_string, end_day_string, period))
            df = read_csv_query_log(start_day_string, end_day_string, dir_dict)
        except Exception as e:
            logger.info(
                "Exception happened for period {}_{} with step: {}".format(start_day_string, end_day_string, period))
            logger.debug("Exception {}".format(e))
            insert_logger.info(
                "Fail the job for period: {}_{} with step: {}".format(start_day_string, end_day_string, period))
            sys.exit(1)
        logger.info("End reading task for the batch: {}_{}".format(start_day_string, end_day_string))
        if df is not None:
            if len(df) > 0:
                logger.info("Start to insert for the batch: {}_{}".format(start_day_string, end_day_string))
                is_successful = bulk_insert_for_a_batch(df)
                print(is_successful)
                if is_successful is None:
                    logger.info("There may be an exception for the batch: {}_{}".format(start_day_string, end_day_string))
                    insert_logger.info(
                        "Fail the job for period: {}_{} with step: {}".format(start_day_string, end_day_string, period))
                elif is_successful:
                    logger.info("End insert task for the batch: {}_{}".format(start_day_string, end_day_string))
                    insert_logger.info(
                        "Finish the job for period: {}_{} with step: {}".format(start_day_string, end_day_string, period))
                else:
                    logger.info("Mysql constraint violation happened in batch: {}_{}".format(start_day_string, end_day_string))
                    logger.info("End insert task for the batch: {}_{}".format(start_day_string, end_day_string))
                    insert_logger.info(
                        "Fail the job for period: {}_{} with step: {}".format(start_day_string, end_day_string, period))
            else:
                logger.info("There may be no app for the batch: {}_{}".format(start_day_string, end_day_string))
                insert_logger.info(
                    "Finish the job for period: {}_{} with step: {}".format(start_day_string, end_day_string, period))
        else:
            logger.info(
                "Unknown error happened for period {}_{} with step: {}".format(start_day_string, end_day_string, period))
            insert_logger.info(
                "Fail the job for period: {}_{} with step: {}".format(start_day_string, end_day_string, period))
            sys.exit(1)

if __name__ == "__main__":
    main(sys.argv[1:])
