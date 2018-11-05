#!/usr/bin/env python
import sys
import argparse
# import ConfigParser
from cm_api.api_client import ApiResource
# from cm_api.endpoints.services import ApiService

# cm_host = "cm-host"
# api = ApiResource(cm_host, username="admin", password="admin")

# # Get a list of all clusters
# cdh4 = None
# for c in api.get_all_clusters():
  # print c.name
  # if c.version == "CDH4":
    # cdh4 = c

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("action", help="stop or start the cluster")
	args = parser.parse_args()
	action = args.action
	
	if (args.action == "start"):
		print " ".join([action,"cluster"])
		# cluster.start().wait()
	elif (args.action == "stop"):
		print " ".join([action,"cluster"])
		# cluster.stop().wait()
	else:
		print ("Action can be either start or stop")
		sys.exit(1)
	


	# # Prep for reading config props from external file
	# CONFIG = ConfigParser.ConfigParser()
	# CONFIG.read("clouderaconfig.ini")

	# ### Set up environment-specific vars ###

	# # This is the host that the Cloudera Manager server is running on
	# CM_HOST = CONFIG.get("CM", "cm.host")

	# # CM admin account info
	# ADMIN_USER = CONFIG.get("CM", "admin.name")
	# ADMIN_PASS = CONFIG.get("CM", "admin.password")

	# #### Cluster Definition #####
	# CLUSTER_NAME = CONFIG.get("CM", "cluster.name")
	# CDH_VERSION = "CDH5"

	# API = ApiResource(CM_HOST, version=5, username=ADMIN_USER, password=ADMIN_PASS)
	# print "Connected to CM host on " + CM_HOST

	# CLUSTER = API.get_cluster(CLUSTER_NAME)

	# print "About to restart cluster."
	# # CLUSTER.restart().wait()
	# print "Done restarting cluster."


if __name__ == "__main__":
   main()
