#!/usr/bin/python3

import datetime
import os
import sys
import traceback


class MyTinyLogger(object):
    """My Tiny Logger for EDSF and other project"""

    def __init__(self, log_filename, err_filename, maxsize=400000, maxchunks=8, silentstart=False):
        """Constructor"""
        self.silentstart = silentstart
        self.logfilename = log_filename
        self.errfilename = err_filename
        self.maxsize = maxsize
        self.maxchunks = maxchunks
        self.rotatelog()

    def rotatelog(self):
        if os.path.exists(self.logfilename):
            if os.path.getsize(self.logfilename) > self.maxsize:
                log_dir = os.path.split(self.logfilename)[0]
                log_name = os.path.split(self.logfilename)[1]
                date = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                name = log_name.split('.')
                newname = log_dir + '/' + name[0] + '_' + date + '.log'
                os.rename(self.logfilename, newname)

        else:
            logfile = open(self.logfilename, "a")
            if self.silentstart is False:
                logfile.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " -> Log file open\n")
            logfile.close()

        if os.path.exists(self.errfilename):
            if os.path.getsize(self.errfilename) > self.maxsize:
                log_dir = os.path.split(self.errfilename)[0]
                log_name = os.path.split(self.errfilename)[1]
                date = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                name = log_name.split('.')
                newname = log_dir + '/' + name[0] + '_' + date + '.log'
                os.rename(self.errfilename, newname)
        else:
            logfile = open(self.errfilename, "a")
            if self.silentstart is False:
                logfile.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " -> Error file open\n")
            logfile.close()

    def log(self, message, needprint=True):
        self.rotatelog()
        logfile = open(self.logfilename, "a")
        logfile.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " -> " + message + "\n")
        logfile.close()
        if needprint:
            print(message)

    def log_err(self, ):
        self.rotatelog()
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print("TraceBack:")
        traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
        print("Exception:")
        tr_back = traceback.format_exception(etype=exc_type, value=exc_value, tb=exc_traceback, chain=True)
        tr_back_str = ""
        for i in tr_back:
            tr_back_str = tr_back_str + i + "\n"
        with open(self.errfilename, "a") as logfile:
            logfile.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "   ----------------------------\nERROR TRACEBACK -> \n" + tr_back_str)
        logfile.close()
        print(tr_back_str)
