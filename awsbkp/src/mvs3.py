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
from io import StringIO, BytesIO
import hexdump
import binascii

def upload(s3, mpu_id, bucket, fpath, s3_fpath, part_bytes):
	parts = []
	with open(fpath, "rb") as f:
		i = 1
		while True:
			data = f.read(part_bytes)
			if not len(data):
				break
			part = s3.meta.client.upload_part(Body=data, Bucket=bucket, Key=s3_fpath, UploadId=mpu_id, PartNumber=i)
			parts.append({"PartNumber": i, "ETag": part["ETag"]})
			i += 1
	return parts

def multi_part_upload_with_s3(fpath, s3, bucket, s3_ddir):
	s3_fpath = os.path.join(s3_ddir, os.path.basename(fpath))
	part_bytes = int(15e6) # split in 15mb chunks
	mpu = s3.meta.client.create_multipart_upload(Bucket=bucket, Key=s3_fpath)
	mpu_id = mpu["UploadId"]
	parts = upload(s3, mpu_id, bucket, fpath, s3_fpath, part_bytes)
	s3.meta.client.complete_multipart_upload(Bucket=bucket, Key=s3_fpath, UploadId=mpu_id, MultipartUpload={"Parts": parts})
	etags = [ elem["ETag"] for elem in parts]
	return etags


def find_etag(s3, bucket, s3_fpath):
	try:
		resp = s3.meta.client.head_object(Bucket=bucket, Key=s3_fpath)
		return resp['ETag'].strip('"')
	except:
		logger.error("Exception occurred while calculating S3 Etag: ", sys.exc_info()[0])

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

	#AWS etag algorithm : xxd -r -p f1 | md5sum where f1 is a file containing all the hashes of the parts
	for f in out.splitlines():
		logger.info("Started copying " + f + " to " + s3_ddir)
		etags = multi_part_upload_with_s3(f, s3, bucket, s3_ddir)
		logger.info("Ended copying " + f)

		logger.info("Calculating file ETag based on parts ETags " + f)
		xxd_f = os.path.join("/tmp", os.path.basename(f))
		with open(xxd_f, "w") as outf:
			for i, etag in enumerate(etags):
				outf.write(etag.strip('\"'))
		out, ret = run.run_cmd(["xxd", "-r", "-p", xxd_f], logger)
		calc_etag = hashlib.md5(out.encode("ISO-8859-1")).hexdigest() + "-" + str(len(etags))
		logger.info("Calculated ETag for " + f + " is " + calc_etag)
		s3_fpath = os.path.join(s3_ddir, os.path.basename(f))
		s3_etag = find_etag(s3, bucket, s3_fpath)
		logger.info("ETag returned by S3 API for " + s3_fpath + " is " + s3_etag)
		if(s3_etag == calc_etag):
			logger.info("Calculated ETag and ETag returned by S3 API match")
	#		logger.info("Removing tar file " + file)
	#		out, ret = run.run_cmd(["rm", "-f", file], logger)

if __name__ == "__main__":
	main()
