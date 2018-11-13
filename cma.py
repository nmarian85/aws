#!/usr/bin/env python
import sys
import argparse
import ConfigParser
import ssl
import time
import multiprocessing
from cm_api.api_client import ApiResource
import glob
import subprocess as sp
import timeit
import socket
import signal
import smtplib
from email.mime.text import MIMEText
import datetime
import csv
import json
import re
import pprint
import logging
import logging.config

def run_cmd(cmd, logger):
	try:
		p = sp.Popen(cmd, stdout = sp.PIPE, stderr = sp.PIPE)
		out, err = p.communicate()
		if err:
			 logger.error("Error: ", err.strip())
	except OSError as e:
		 logger.error("Error: " + e.strerror)
	except:
		 logger.error("Error: ", sys.exc_info()[0])
	# p_status = p.wait()
	# print("Command output : " + output
	# print("Command exit status/return code : ", p_status
	return out.decode("utf-8")

def get_state(object, state, component):
	retval = True
	if (component == "aws"):
		out = run_cmd(["aws", "--profile", "director-aws-user", "ec2", "describe-instance-status", "--include-all-instances"])
		instdict = json.loads(out)
		for d in instdict["InstanceStatuses"]:
			id = d["InstanceId"]
			if id not in object:
				continue
			# print(d["InstanceId"] )
			# print(d["InstanceState"]["Name"] )
			if (d["InstanceState"]["Name"] == state):
				 logger.info(id + " in state " + d["InstanceState"]["Name"])
			else:
				logger.info(id + " not " + state + " yet")
				retval = False				

	elif (component == "cdh"):
		for s in object.get_all_services():
			n = s.name
			st = s.serviceState
			# print(n + " " + st)
			if "CDSW" in n:
				continue
			if st == state:
				logger.info(n + " in state " + st)
			else:
				logger.info(n + " not " + state + " yet")
				retval = False
	logger.info("====================================")
	return retval
	
def run_process(farg, action, state, component, logger):
	p = multiprocessing.Process(target=do_work, args=(farg, action, component))
	p.start()
	
	timeout = time.time() + 60*15 # 15 minutes
	# timeout = time.time() + 10
	while True:
		if (get_state(farg, state, component) == True):
			logger.info("Everything ok with " + component)
			p.terminate()
			break
		if (time.time() > timeout):
			logger.error("ERROR: timeout reached ")
			p.terminate()
			sys.exit(1)
		time.sleep(10)
		# time.sleep(1)
	# p.join()

def run_aws(action, aws_st, logger):
	component = "aws"
	aws_cdh_inst = ["i-0c7750a3b6cb107fa", "i-0ecf8e69d51828c41", "i-0fef039957a700c95", "i-00b8e90d0b3cf58a5", "i-03821bf0a5fb2b34b", "i-03c6d6779ada0806c", "i-0e8d0ffdb9e11fe00"]
	run_process(aws_cdh_inst, action, aws_st, "aws", logger)

def run_cdh(action, cdh_st, logger):
	component = "cdh"
	cfg = ConfigParser.ConfigParser()
	cfg.read("cdh.cfg")
	
	cmhost = cfg.get("CM", "host")
	user = cfg.get("CM", "username")
	passw = cfg.get("CM", "password")
	cafile = cfg.get("CM", "cafile")
	
	context = ssl.create_default_context(cafile=cafile)

	api = ApiResource(cmhost, username=user, password=passw, ssl_context = context, use_tls = True)	
	allc = api.get_all_clusters()
	c = allc[0]
	run_process(c, action, cdh_st, component, logger)

def do_work(object, action, component, logger):
	if (component == "cdh"):
		cluster = object
		if (action == "start"):
			logger.info("Starting cluster")
			cluster.start().wait()
		elif (action == "stop"):
			logger.info("Stopping cluster")
			cluster.stop().wait()
	elif (component == "aws"):
		aws_cdh_inst = object
		if (action == "start"):
			logger.info("Starting AWS instances")
			for id in aws_cdh_inst:
				run_cmd(["aws", "--profile", "director-aws-user", "ec2", "start-instances", "--instance-ids", id])
		elif (action == "stop"):
			logger.info("Stopping AWS instances")
			for id in aws_cdh_inst:
				run_cmd(["aws", "--profile", "director-aws-user", "ec2", "stop-instances", "--instance-ids", id])	   

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("action", help="stop or start the cluster")
	args = parser.parse_args()
	action = args.action
	logging.config.fileConfig('logging.conf')
	logger = logging.getLogger('aws_cm_actions')


	if ((action != "start") and (action != "stop")):
		print ("Action can be either start or stop")
		sys.exit(1)
	
	logger.info ("Script started")
	# if (action == "start"):
		# cdh_st = "STARTED"
		# aws_st = "running"
		# run_aws(action, aws_st, logger)
		# time.sleep(180)
		# run_cdh(action, cdh_st, logger)
	# elif (action == "stop"):
		# cdh_st = "STOPPED"
		# aws_st = "stopped"
		# run_cdh(action, cdh_st, logger)
		# run_aws(action, aws_st, logger)
	logger.info ("Script ended")

if __name__ == "__main__":
	main()
	