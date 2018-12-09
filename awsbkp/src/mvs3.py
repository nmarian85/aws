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
import boto3
from boto3.s3.transfer import TransferConfig
from filechunkio import FileChunkIO

def multi_part_upload_with_s3(f, s3, bucket, s3_ddir, logger):
	config = TransferConfig(multipart_threshold=1024 * 25, max_concurrency=10, multipart_chunksize=1024 * 25, use_threads=True)
#	bucket = dst_path.split("/")[0]
#	s3_dir = dst_path[dst_path.index("/") + 1:]	
	s3_path = os.path.join(s3_ddir, os.path.basename(f))
	logger.info("Copying  " + f + " to " + s3_path)
	s3.meta.client.upload_file(Filename=f, Bucket=bucket, Key=s3_path, Config=config)
#l                            ExtraArgs={'ACL': 'public-read', 'ContentType': 'text/pdf'},
#                            Config=config,
#                            Callback=ProgressPercentage(file_path)
#                            )

def calc_md5(f, logger):
	with open(f, "rb") as binary_file:
		data = binary_file.read()
	md5 = hashlib.md5(data).hexdigest()
	logger.info("md5sum of " + f + " is " + str(md5))
	return md5

def get_etag(s3_f, bucket, logger):
#aws s3api head-object --bucket s3-pdapilot-storage --key s3-scrappy-backup/edeka_product_one_2018_11_25__00.tgz
	s3md5 = ""
	out, ret = run.run_cmd(["aws", "s3api", "head-object", "--bucket", bucket, "--key", s3_f], logger)
	for l in out.splitlines():
		logger.info(l)
		cols = l.split(":")
		if (len(cols) >= 2 and "ETag" in l):
			s3md5 = cols[1].replace('\\','').replace('"','').replace(' ','')
	return s3md5

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
	code_dir = os.path.join(os.environ['HOME'], "aws", "awsbkp")
	logging.config.fileConfig(os.path.join(code_dir, "conf", "loggingmvs3.conf"))
	logger = logging.getLogger()
	
	config = configparser.RawConfigParser()
	config.read(os.path.join(code_dir, "conf", "mvs3.conf"))

	sdir = config['DEFAULT']['sdir']
	s3_ddir = config['DEFAULT']['s3_ddir']
	since = config['DEFAULT']['since']
	until = config['DEFAULT']['until']
	bucket = config['DEFAULT']['bucket']

	since_d = eval(since)
	until_d = eval(until)
	s3 = boto3.resource('s3')
	
	out, ret = run.get_files(sdir, since_d, until_d, logger)

	for f in out.splitlines():
		fmd5 = calc_md5(f, logger)
		multi_part_upload_with_s3(f, s3, bucket, s3_ddir, logger)
		s3_fmd5 = get_etag(os.path.join(s3_ddir, os.path.basename(f)), bucket, logger)
		print(s3_fmd5)
	#	ret = mvs3(file, dst_path, logger)
	#	if(ret == 0):
	#		logger.info("Removing tar file " + file)
	#		out, ret = run.run_cmd(["rm", "-f", file], logger)

if __name__ == "__main__":
	main()
