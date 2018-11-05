#!/usr/bin/env python
import sys
import argparse
import ConfigParser
import ssl
from cm_api.api_client import ApiResource

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("action", help="stop or start the cluster")
	args = parser.parse_args()
	action = args.action
		
	if (args.action == "start"):
		print " ".join([action +"ing" ,"cluster"])
		# cluster.start().wait()
	elif (args.action == "stop"):
		print " ".join([action+"ping","cluster"])
		# cluster.stop().wait()
	else:
		print ("Action can be either start or stop")
		sys.exit(1)

	cfg = ConfigParser.ConfigParser()
	cfg.read("cdh.cfg")
	
	cmhost = cfg.get("CM", "host")
	user = cfg.get("CM", "username")
	passw = cfg.get("CM", "password")
	cafile = cfg.get("CM", "cafile")
	
	context = ssl.create_default_context(cafile=cafile)

	api = ApiResource(cmhost, username=user, password=passw, ssl_context = context, use_tls = True)	
	for c in api.get_all_clusters():
		print c.name

if __name__ == "__main__":
   main()
