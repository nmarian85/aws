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

def run_cmd(cmd):
	try:
		p = sp.Popen(cmd, stdout = sp.PIPE, stderr = sp.PIPE)
		out, err = p.communicate()
		if err:
			print("Error: ", err.strip())
	except OSError as e:
		print("Error: " + e.strerror)
	except:
		print("Error: ", sys.exc_info()[0])
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
				print (id + " in state " + d["InstanceState"]["Name"])
			else:
				print(id + " not " + state + " yet")
				retval = False				
		return True
	elif (component == "cdh"):
		for s in object.get_all_services():
			n = s.name
			st = s.serviceState
			# print(n + " " + st)
			if "CDSW" in n:
				continue
			if st == state:
				print(n + " in state " + st)
			else:
				print(n + " not " + state + " yet")
				retval = False
	print "===================================="
	return retval
	
def run_process(farg, action, state, component):
	p = multiprocessing.Process(target=do_work, args=(farg, action, component))
	p.start()
	
	timeout = time.time() + 60*10 # 10 minutes
	# timeout = time.time() + 10
	while True:
		if ((get_state(farg, state, component) == True ) or (time.time() > timeout)):
			print "Everything ok"
			p.terminate()
			break
		time.sleep(10)
		# time.sleep(1)
	# p.join()

def run_aws(action, aws_st):
	component = "aws"
	aws_cdh_inst = ["i-0c7750a3b6cb107fa", "i-0c8199d121a0409ec", "i-0ecf8e69d51828c41", "i-0fef039957a700c95", "i-00b8e90d0b3cf58a5", "i-03821bf0a5fb2b34b", "i-03c6d6779ada0806c", "i-0e8d0ffdb9e11fe00"]
	run_process(aws_cdh_inst, action, state, "aws")

def run_cdh(action, cdh_st):
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
	run_process(c, action, cdh_st, component)

def do_work(object, action, component):
	if (component == "cdh"):
		cluster = object
		if (action == "start"):
			print "Starting cluster"
			cluster.start().wait()
		else:
			print "Stopping cluster"
			cluster.stop().wait()
	elif (component == "aws"):
		pass
	
def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("action", help="stop or start the cluster")
	args = parser.parse_args()
	action = args.action
	
	if ((action != "start") and (action != "stop")):
		print ("Action can be either start or stop")
		sys.exit(1)
	
	if (action == "start"):
		cdh_st = "STARTED"
		aws_st = "running"
		# run_aws(aws_st, "aws")
		run_cdh(action, cdh_st)
	else:
		cdh_st = "STOPPED"
		aws_st = "stopped"
		run_cdh(action, cdh_st)
		# run_aws(aws_st, "aws")
		
if __name__ == "__main__":
	main()
	