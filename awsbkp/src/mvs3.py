#!/usr/bin/env python3
import sys
import os
import time
import glob
import timeit
import socket
import signal
import datetime
import logging
import logging.config
import configparser
import run
import tarfile
import base64
import hashlib
import re

def mvs3(fname, dst_dir, logger):
	bucket = dst_dir.split("/")[0]
	folder = dst_dir.split("/")[1]
	basefname = os.path.basename(fname)
	s3md5 = ""
	
	with open(fname, "rb") as binary_file:
		data = binary_file.read()

	md5 = hashlib.md5(data).hexdigest()
	
	logger.info("md5sum of " + fname + " is " + str(md5))
	logger.info("Copying  " + fname + " to " + dst_dir)
	
	# out, ret = run.run_cmd(["aws", "s3api", "put-object", "--bucket", bucket, "--key", folder + "/" + basefname, "--body", fname, "--metadata", "md5chksum=" + md5, "--content-md5", md5  ], logger)
	out, ret = run.run_cmd(["aws", "s3api", "put-object", "--bucket", bucket, "--key", folder + "/" + basefname, "--body", fname], logger)

	for l in out.splitlines():
		logger.info(l)
		cols = l.split(":")
		if (len(cols) >= 2 and "ETag" in l):
			s3md5 = cols[1].replace('\\','').replace('"','').replace(' ','')

	if (s3md5 == md5):
		logger.info("md5 and ETag match")
		return 0
	return -1

def main():
	sdir = os.path.join(os.environ['HOME'], "aws", "awsbkp")
	logging.config.fileConfig(os.path.join(sdir, "conf", "loggingmvs3.conf"))
	logger = logging.getLogger()
	
	config = configparser.RawConfigParser()
	config.read(os.path.join(sdir, "conf", "mvs3.conf"))
	
	src_path = config['DEFAULT']['src_path']
	dst_path = config['DEFAULT']['dst_path']
	start_day = config['DEFAULT']['start_day']
	end_day = config['DEFAULT']['end_day']

	r_d = eval(end_day)
	s_d = eval(start_day)
	
	out, ret = run.get_files(src_path, s_d, r_d, logger)

	for file in out.splitlines():
		ret = mvs3(file, dst_path, logger)
		if(ret == 0):
			logger.info("Removing tar file " + file)
			out, ret = run.run_cmd(["rm", "-f", file], logger)

			# logger.info("Removing directory " + l)
			# out, ret = run.run_cmd(["rm", "-rf", l], logger)

if __name__ == "__main__":
	main()
