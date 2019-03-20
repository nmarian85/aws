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
import hashlib

def tard(d, tar_loc, logger):
	fname = os.path.join(tar_loc, os.path.basename(d)) + ".tgz"
	logger.info("Tarring directory " + d + " to location " + fname)
	
	with tarfile.open(fname, "w:gz") as tar:
		tar.add(d, arcname = os.path.basename(d))
	return fname
		
def getlsmd5(dir):
	fmd5 = dict()
	for root, dirs, files in os.walk(dir):  
		for filename in files:
			fpath = os.path.join(root, filename)
			fmd5[fpath] = run.calc_fmd5(fpath)
	return fmd5
	
def gettarmd5(tarf):
	tar_fmd5 = dict()
	with tarfile.open(tarf) as t:
		for m in t.getmembers():
			if m.isfile():
				f = t.extractfile(m)
				content = f.read()
				tar_fmd5[m.name] = hashlib.md5(content).hexdigest()
	return tar_fmd5

def cmp(d1, d2):
	for k in d1.keys():
		if(d1[k] != d2[k]):
			return False
	return True
		
def main():
	sdir = os.path.join(os.environ['HOME'], "aws", "awsbkp")
	logging.config.fileConfig(os.path.join(sdir, "conf", "loggingmvd.conf"))
	logger = logging.getLogger()
	
	config = configparser.RawConfigParser()
	config.read(os.path.join(sdir, "conf", "mvd.conf"))
	
	sdir = config['DEFAULT']['sdir']
	ddir = config['DEFAULT']['ddir']
	
	since = config['DEFAULT']['since']
	until = config['DEFAULT']['until']


	since_d = eval(since)
	until_d = eval(until)
	
	out, ret = run.get_dirs(sdir, since_d, until_d, logger)
	
	for dir in out.splitlines():
		fmd5 = getlsmd5(dir)
		# for k, v in fmd5.items():
			# print(k + " " + v)
		tarf = tard(dir, ddir, logger)
		tar_fmd5 = gettarmd5(tarf)
		
		abs_path_tar_fmd5 = dict()
		for k, v in tar_fmd5.items():
			abs_path_tar_fmd5[os.path.join(sdir, k)] = tar_fmd5[k]
		
		if(cmp(fmd5, abs_path_tar_fmd5)): 
			logger.info("md5s match for all " + str(len(fmd5.keys())) + " files so removing source directory " + dir)
			out, ret = run.run_cmd(["rm", "-rf", dir], logger)
		else:
			logger.info("md5s for files dont't match")
	logger.info("Script ended")
if __name__ == "__main__":
	main()
