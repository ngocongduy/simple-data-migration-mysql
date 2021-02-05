import pandas as pd
import os
import mysql
import sys

try:
    from env import PROD_DB_CREDENTIAL
except:
    PROD_DB_CREDENTIAL = {

    }


def insert_bulk(config_values=None, query_tring=None, data=None):
    def _try_query(query_string, data):
        # config params
        if config_values is None:
            print("No config values!")
            return None

        cnx = mysql.connector.connect(**config_values)
        cursor = cnx.cursor()
        is_successful = True
        try:
            cursor.executemany(query_tring, data)
            cnx.commit()
            print("Insert finished! Row count {}".format(cursor.rowcount))
        except mysql.connector.IntegrityError as err:
            print("Violate constraints!")
            is_successful = False
            # Should log into a file
        except mysql.connector.DatabaseError as err:
            print("Other database error, retry!")
            print(err)
            # Should log into a file to retry later
            is_successful = None
        except Exception as e:
            print(e)
            is_successful = None
        finally:
            cursor.close()
            return is_successful

    if query_tring is None:
        print("Query string cannot be none!")
        return None
    elif type(data) is not list:
        print("Data should be a list of item-ordered tuple")
    else:
        print("Try to insert... !")
        try:
            is_successful = _try_query(query_tring, data)
            return is_successful
        except:
            return None


import pandas as pd
from simple_logger import get_logger

logger, dir_dict = get_logger()


def main(argv):
    query_logs_dir = dir_dict.get("query_logs")

    start_day_string = "2019-01-01"
    end_day_string = "2019-01-02"



if __name__ == "__main__":
    main(sys.argv[1:])
