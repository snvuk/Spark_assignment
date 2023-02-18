from core.util import *
from pyspark.sql import SparkSession



spark = sparkSe()
spark = SparkSession

def Log_Df(logfile):
    log_Df = logfile(spark)
    return log_Df



def logs_file(Withcolumn_Log,log_Df):
    logs_file =Withcolumn_Log(log_Df)
    return logs_file



def warn_count(find_warn):
    warn_count = find_warn(logs_file,"Logging")
    return warn_count


def total_line_count(total_line, log_Df):
    total_line_count = total_line(log_Df)
    return total_line_count


def total_Api_Client(api_Client):
    total_Api_Client =api_Client(logs_file)
    return total_Api_Client


def most_Http_count(most_Http):
    most_Http_count = most_Http(logs_file)
    return most_Http_count


def failed_Request_count(faild_Request):
    failed_Request_count = faild_Request(logs_file)
    return failed_Request_count


def most_Active_Hour_Count(most_Acite_Hour):
    most_Active_Hour_Count =most_Acite_Hour(logs_file)
    return most_Active_Hour_Count

def active_Repository_count(most_Active_Repository):
    active_Repository_count = most_Active_Repository(logs_file)
    return active_Repository_count
