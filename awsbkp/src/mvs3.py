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

def mvs3(fname, dst_dir, logger):
	#openssl md5 -binary /var/tmp/test/tars/test1.tgz | base64
	with open(fname, "rb") as binary_file:
		data = binary_file.read()

	dig = hashlib.md5(data).digest()
	md5sum = base64.b64encode(dig)
	md5 = md5sum.decode()
	
	logger.info("md5sum base64 of " + fname + " is " + md5)
	
	logger.info("Copying  " + fname + " to " + dst_dir)
	# out, ret = run.run_cmd(["aws", "s3", "cp", fname, dst_dir], logger)
	
	bucket = dst_dir.split("/")[0]
	folder = dst_dir.split("/")[1]
	basefname = os.path.basename(fname)
	out, ret = run.run_cmd(["aws", "s3api", "put-object", "--bucket", bucket, "--key", folder + "/" + basefname, "--body", fname, "--metadata", "md5chksum=" + md5, "--content-md5", md5  ], logger)

	#check for return etag
	#md5sum check to be done
	for l in out.splitlines():
		logger.info(l)
	return ret

def tard(d, tar_loc, logger):
	logger.info("Tarring " + d)
	fname = os.path.join(tar_loc, os.path.basename(d)) + ".tgz"
	with tarfile.open(fname, "w:gz") as tar:
		tar.add(d, arcname = os.path.basename(d))
	return fname

def main():
	sdir = os.path.join(os.environ['HOME'], "aws", "awsbkp")
	logging.config.fileConfig(os.path.join(sdir, "conf", "loggingmvs3.conf"))
	logger = logging.getLogger()
	
	config = configparser.RawConfigParser()
	config.read(os.path.join(sdir, "conf", "mvs3.conf"))
	
	src_path = config['DEFAULT']['src_path']
	dst_path = config['DEFAULT']['dst_path']
	run_day = config['DEFAULT']['run_day']
	since_day = config['DEFAULT']['since_day']

	r_d = eval(run_day)
	s_d = eval(since_day)
	
	tar_loc = os.path.join(src_path, "tars")
	out, ret = run.get_dirs(src_path, s_d, r_d, logger)

	for l in out.splitlines():
		if (l == tar_loc):
			continue
			
		fname = tard(l, tar_loc, logger)		
		ret = mvs3(fname, dst_path, logger)
		# if(ret == 0):
			# logger.info("Removing tar file " + fname)
			# out, ret = run.run_cmd(["rm", "-f", fname], logger)

			# logger.info("Removing directory " + l)
			# out, ret = run.run_cmd(["rm", "-rf", l], logger)


if __name__ == "__main__":
	main()
	