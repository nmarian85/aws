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

def mvd(dir, dst_dir, logger):
	logger.info("Moving directory " + dir + " to " + dst_dir)
	out, ret = run.run_cmd(["rsync", "-a", dir, dst_dir], logger)
	if(ret == 0):
		out, ret = run.run_cmd(["rm", "-rf", dir], logger)
	# out = run.run_cmd(["rsync", "-a", "--remove-source-files", "--dry-run", dir, dst_dir], logger)
	# out = run.run_cmd(["rsync", "-a", "--remove-source-files", dir, dst_dir], logger)

def main():
	sdir = os.path.join(os.environ['HOME'], "aws", "awsbkp")
	logging.config.fileConfig(os.path.join(sdir, "conf", "loggingmvd.conf"))
	logger = logging.getLogger()
	
	config = configparser.RawConfigParser()
	config.read(os.path.join(sdir, "conf", "mvd.conf"))

	logger.info("Move started")
	
	src_path = config['DEFAULT']['src_path']
	dst_path = config['DEFAULT']['dst_path']
	run_day = config['DEFAULT']['run_day']
	since_day = config['DEFAULT']['since_day']

	r_d = eval(run_day)
	s_d = eval(since_day)
	
	out, ret = run.get_dirs(src_path, s_d, r_d, logger)
	for l in out.splitlines():
		mvd(l, dst_path, logger)
	
if __name__ == "__main__":
	main()
