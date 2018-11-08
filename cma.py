#!/usr/bin/env python
import sys
import argparse
import ConfigParser
import ssl
import time
import multiprocessing
from cm_api.api_client import ApiResource

def get_svc_action(c, action):
	for s in c.get_all_services():
		n = s.name
		st = s.serviceState
		print(n + " " + st)
		if "CDSW" in n:
			continue
		if action not in st.lower():
			print(n + " not in " + action + " state yet")
			return False
	return True
		

def do_actions(c, action):
	if (action == "start"):
		print("Starting cluster")
		c.start().wait()
	else:
		print ("Stopping cluster")
		c.stop().wait()

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("action", help="stop or start the cluster")
	args = parser.parse_args()
	action = args.action
	
	if ((action != "start") and (action != "stop")):
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
	allc = api.get_all_clusters()
	c = allc[0]
	
	p = multiprocessing.Process(target=do_actions, args=(c, action))
	p.start()
	
	timeout = time.time() + 60*10
	while True:
		if ((get_svc_action(c, action) == True ) or (time.time() > timeout)):
			p.join()
			break
		time.sleep(10)
	p.join()
	
if __name__ == "__main__":
	main()
	