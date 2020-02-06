#!/bin/python3
# -*- coding: utf-8 -*-
from mylogger.factory import StdoutLoggerFactory, \
                             FileLoggerFactory, \
                             RotationLoggerFactory
import os
import sys
import time
import threading
import os.path
import boto3

AWS_CREDENTIAL_PROFILE = 'default'
AWS_REGION = 'ap-northeast-1'
LOG_BASEPATH = r'/var/log'
LOG_HANDLER = 'rotation'
LOGLEVEL = 20
LOGFILE = 'S3Operation.log'


class S3Uploader(threading.Thread):
    """[summary]
    
    Args:
        object ([type]): [description]
    """
    def __init__(self,
                 bucket,
                 aws_cred_secname=None,
                 aws_accesskey=None,
                 aws_secretkey=None,
                 logpath=None,
                 loglevel=None,
                 logger=None,
                 handler=None,
                 aws_region=None):
        """S3Uploader class constructor
            bucket ([str]): target bucket name.
            aws_cred_secname ([type], optional): Defalts to None.
                section name of aws credential file ~/.aws/credentials | config
            loglevel ([str], optional): Defaults to None. logging level
            logpath ([str], optional): Defaults to None.
                path to logging. if not specified, sets path to /var/log .
            logger ([Logger], optional): Defaults to None.
                logger object. if not specified, sets logger depending on handler.
            handler ([str], optional): Defaults to None.
                settings the logging handler.
                a valid value is 'file' | 'console' | 'rotation'
            aws_region ([str], optional): Defaults is ap-northeast-1
                your aws s3 bucket region name.
        """
        if logpath is None:
            logpath = LOG_BASEPATH
        if loglevel is None:
            loglevel = LOGLEVEL
        if handler is None:
            handler = LOG_HANDLER
        if aws_cred_secname is None:
            aws_cred_secname = AWS_CREDENTIAL_PROFILE
        if aws_region is None:
            aws_region = AWS_REGION
        self._loglevel = loglevel
        self._handler = handler
        self._logpath = os.path.join(logpath, LOGFILE)
        self._aws_cred_secname = aws_cred_secname
        self.__aws_accesskey = aws_accesskey
        self.__aws_secretkey = aws_secretkey
        self.aws_region = aws_region
        self._session_args = {
            "profile_name": self._aws_cred_secname,
            "region_name": self.aws_region
        }
        self._session_args_usekey = {
            "aws_access_key_id": self.__aws_accesskey,
            "aws_secret_access_key": self.__aws_secretkey,
            "region_name": self.aws_region
        }
        ### create a new session
        if self.__aws_accesskey is None and self.__aws_secretkey is None:
            session = boto3.session.Session(**self._session_args)
        else:
            session = boto3.session.Session(**self._session_args_usekey)
        self.__s3 = session.resource('s3')
        self.__bucket = self.__s3.Bucket(bucket)

        ### make log directory
        if os.path.isdir(logpath):
            os.makedirs(logpath, exist_ok=True)
        ### create logger
        if logger is None:
            if self._handler == 'file':
                flogger_fac = FileLoggerFactory(logger_name=__name__,
                                                loglevel=self._loglevel)
                self._logger = flogger_fac.create(self._logpath)
            elif self._handler == 'console':
                stdlogger_fac = StdoutLoggerFactory(logger_name=__name__,
                                                    loglevel=self._loglevel)
                self._logger = stdlogger_fac.create()
            elif self._handler == 'rotation':
                rlogger_fac = RotationLoggerFactory(logger_name=__name__,
                                                    loglevel=self._loglevel)
                self._logger = rlogger_fac.create(self._logpath,
                                                  bcount=10)
            else:
                sys.stderr.write("an invalid value of logger handler " \
                                 "was thrown '{}' ." \
                                 " a valid values are console, file, rotation".format(self._handler))
                sys.stderr.flush()
        else:
            self._logger = logger

    def upload(self, src_path: str, key_name=None, **extra_args):
        """[summary]
        
        Args:
            src_path (str): target file to upload
            key_name (str)[optional]: object key name on amazon s3
            extra_args (various)[optional]: extra keyword arguments.
                the list of available keywords is specified in the boto3.s3.transfer.S3Transfer.ALLOWED_UPLOAD_ARGS
        """
        ex_args = dict()
        ex_args = extra_args
        
        # filename to use by default
        if key_name is None:
            key_name = os.path.split(src_path)[1]
        
        # check ExtraArgs
        if ex_args:
            if 'Metadata' in ex_args and not isinstance(ex_args['Metadata'], dict):
                raise ValueError('Metadata')
        else:
            ex_args = None
        
        #data = open(src_path, 'rb')
        self.__bucket.upload_file(src_path,
                                  key_name,
                                  ExtraArgs=ex_args,
                                  Callback=ProgressPercentage(src_path,
                                                              logger=self._logger))

class ProgressPercentage(object):
    """[summary]
    
    Args:
        object ([type]): [description]
    """
    def __init__(self, filename, logger=None):
        self._start_time = time.time()
        self._logger = logger
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()
        if self._logger is not None:
            self._logger.name = __name__

    def __call__(self, bytes_amount):
        elapsed_time = time.time() - self._start_time
        with self._lock:
            self._seen_so_far += bytes_amount
            percentages = (self._seen_so_far / self._size) * 100
            msg = "\r%s  %s / %s  (%.2f%%) %.2fsec" % (
                    self._filename, self._seen_so_far, self._size, percentages,
                    elapsed_time
                    )
            if self._logger is not None:
                self._logger.info(msg)
            else:
                sys.stdout.write(msg + "\n")
                sys.stdout.flush()
