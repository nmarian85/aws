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

def mvs3(dir, dst_dir, logger):
	logger.info("Moving directory " + dir + " to " + dst_dir)
	out, ret = run.run_cmd(["aws", "sync", dir, dstdir], logger)
	if(ret == 0):
		out, ret = run.run_cmd(["rm", "-rf", dir], logger)
	# out = run.run_cmd(["rsync", "-a", "--remove-source-files", "--dry-run", dir, dst_dir], logger)
	# out = run.run_cmd(["rsync", "-a", "--remove-source-files", dir, dst_dir], logger)

	# def sync(dir, since_day, run_day, logger):
	# print("aws s3 sync " + dir + " s3://s3-pdapilot-storage")

def tard(d, tar_loc, logger):
	logger.info("Tarring " + d)
	fname = os.path.join(tar_loc, os.path.basename(d)) + ".tgz"
	with tarfile.open(fname, "w:gz") as tar:
		tar.add(d)

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
	print(out)
	for l in out.splitlines():
		if (l == tar_loc):
			continue
		tard(l, tar_loc, logger)
		# mvs3(l, dst_path, logger)
if __name__ == "__main__":
	main()