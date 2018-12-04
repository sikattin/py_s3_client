#!/bin/python3
# -*- coding: utf-8 -*-
from mylogger.factory import StdoutLoggerFactory, \
                             FileLoggerFactory, \
                             RotationLoggerFactory
import os
import sys
import threading
import os.path
import boto3
from s3transfer.upload import UploadSubmissionTask

AWS_CREDENTIAL_PROFILE = 'default'
LOG_BASEPATH = r'/var/log'
LOGFILE = 'S3Operation.log'


class S3Uploader(threading.Thread):
    """[summary]
    
    Args:
        object ([type]): [description]
    """
    def __init__(self,
                 bucket,
                 aws_cred_secname=None,
                 logpath=None,
                 loglevel=None,
                 logger=None,
                 handler=None):
        """[summary]
            bucket ([str]): [Bucket name.]
            aws_cred_secname ([type], optional): Defaults to None. [description]
            loglevel ([type], optional): Defaults to None. [description]
            logpath ([type], optional): Defaults to None. [description]
            logger ([type], optional): Defaults to None. [description]
            handler ([type], optional): Defaults to None. [description]
        """
        if logpath is None:
            logpath = LOG_BASEPATH
        if loglevel is None:
            loglevel = 20
        if handler is None:
            handler = 'rotation'
        if aws_cred_secname is None:
            aws_cred_secname = AWS_CREDENTIAL_PROFILE
        self._loglevel = loglevel
        self._handler = handler
        self._logpath = os.path.join(logpath, LOGFILE)
        self._aws_cred_secname = aws_cred_secname
        
        if self._aws_cred_secname == 'default':
            session = boto3.session.Session()
        else:
            session = boto3.session.Session(profile_name=self._aws_cred_secname)
        self.__s3 = session.resource('s3')
        self.__bucket = self.__s3.Bucket(bucket)

        # make log directory
        if os.path.isdir(logpath):
            os.makedirs(logpath, exist_ok=True)
        # create logger
        if logger is None:
            if self._handler == 'file':
                flogger_fac = FileLoggerFactory(logger_name=__name__,
                                                loglevel=self._loglevel)
                self._logger = flogger_fac.create(file=self._logpath)
            elif self._handler == 'console':
                stdlogger_fac = StdoutLoggerFactory(logger_name=__name__,
                                                    loglevel=self._loglevel)
                self._logger = stdlogger_fac.create()
            elif self._handler == 'rotation':
                rlogger_fac = RotationLoggerFactory(logger_name=__name__,
                                                    loglevel=self._loglevel)
                self._logger = rlogger_fac.create(file=self._logpath,
                                                bcount=10)
        else:
            self._logger = logger

    def upload(self, src_path: str, key_name=None):
        """[summary]
        
        Args:
            src_path (str): [target file to upload]
            key_name (str): [key_name on amazon s3]
        """
        if key_name is None:
            key_name = os.path.split(src_path)[1]
        #data = open(src_path, 'rb')
        self.__bucket.upload_file(src_path,
                                  key_name,
                                  Callback=ProgressPercentage(src_path,
                                                              logger=self._logger))

class ProgressPercentage(object):
    """[summary]
    
    Args:
        object ([type]): [description]
    """
    def __init__(self, filename, logger=None):
        self._logger = logger
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()
        if self._logger is not None:
            self._logger.name = __name__

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            percentages = (self._seen_so_far / self._size) * 100
            msg = "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size, percentages
                    )
            if self._logger is not None:
                self._logger.info(msg)
            else:
                sys.stdout.write(msg)
                sys.stdout.flush
