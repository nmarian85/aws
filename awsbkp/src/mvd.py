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

def tard(d, tar_loc, logger):
	fname = os.path.join(tar_loc, os.path.basename(d)) + ".tgz"
	logger.info("Tarring directory " + d + " to location " + fname)
	
	with tarfile.open(fname, "w:gz") as tar:
		tar.add(d, arcname = os.path.basename(d))

def main():
	sdir = os.path.join(os.environ['HOME'], "aws", "awsbkp")
	logging.config.fileConfig(os.path.join(sdir, "conf", "loggingmvd.conf"))
	logger = logging.getLogger()
	
	config = configparser.RawConfigParser()
	config.read(os.path.join(sdir, "conf", "mvd.conf"))
	
	src_path = config['DEFAULT']['src_path']
	dst_path = config['DEFAULT']['dst_path']
	
	start_day = config['DEFAULT']['start_day']
	end_day = config['DEFAULT']['end_day']

	start = eval(start_day)
	end = eval(end_day)
	
	out, ret = run.get_dirs(src_path, start, end, logger)

	for dir in out.splitlines():
		tard(dir, dst_path, logger)
		logger.info("Removing directory " + dir)
		out, ret = run.run_cmd(["rm", "-rf", dir], logger)
		# if(ret == 0):
			# logger.info("Removing tar file " + fname)
			# out, ret = run.run_cmd(["rm", "-f", fname], logger)

			# logger.info("Removing directory " + l)
			# out, ret = run.run_cmd(["rm", "-rf", l], logger)


if __name__ == "__main__":
	main()
