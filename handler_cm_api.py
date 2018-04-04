#!/usr/bin/python

############################################################################################
#    This file is part of Cloudera_Menu_Commandline.
#
#    Cloudera_Menu_Commandline is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    Cloudera_Menu_Commandline is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
############################################################################################


from cm_api.api_client import ApiResource
from datetime import datetime, timedelta
import subprocess
import xml.etree.ElementTree
import getpass
import os
import re
import sys
import grp
import time
###############################
# Colors
###############################
class COLORS:
	RED = "\x1b[91m"
	BLUE = "\x1b[94m"
	YELLOW = "\x1b[93m"
	GREEN = "\x1b[92m"
	RESET = "\x1b[0m"
def coloring(v_st, v_msg):
        v_color = COLORS.GREEN if v_st in ("STARTED", "GOOD", "MantMode") else COLORS.RED if v_st in ("STOPPED", "BAD") else COLORS.BLUE if v_st in ("INFO", "NA") else COLORS.YELLOW

        v_status = "[" + v_color + v_st + COLORS.RESET + "]"
        return v_status.ljust(20, ' ') + v_msg

###################################################
# Bar for checking the progress till the timeout
###################################################
def progress(count, total, status=''):
    bar_len = 30
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)

    sys.stdout.write('[%s] %s%s --- %s\r' % (bar, percents, '%', status))
    sys.stdout.flush() 

###############################################################
# Uses the Bar of progress and wait for the process to finnish
###############################################################
def f_waiting_task(p_cmd):
	v_index = 0
	v_total=300
	while p_cmd.active and v_index < v_total:
        	progress(v_index, v_total, 'Timeout')
               	p_cmd = p_cmd.wait(1)
	        v_index += 1
        os.system('clear')
	return ['GOOD', 'Task succeded'] if p_cmd.success else ['BAD', 'Task failed']



#################################################################################
# Class for using the Cloudera API
#################################################################################
class handler_cm_api:
	def __init__(self):
		self._user_executing = grp.getgrnam(getpass.getuser())[0]

	def __getitem__(self):
		return self

	def setup(self, p_cm_host, p_cm_user, p_cm_pass, p_cm_version, p_cluster, p_cm_port=None, p_use_tls=False):
		self.cm_api = ApiResource(p_cm_host, server_port=p_cm_port, version=p_cm_version, username=p_cm_user, password=p_cm_pass, use_tls=p_use_tls)
		handler_cm_api.cluster_hosts = self.cm_api.get_all_hosts()
		if p_cluster:
			self.cluster = filter(lambda x: x.displayName == p_cluster, self.cm_api.get_all_clusters())[0]
			if not self.cluster:
				print("Error: That cluster is not valid.")
				return
			else:
				self.services = self.cluster.get_all_services()
				self.name = self.cluster.displayName

		tmp_topology = self.cluster.list_hosts()
		self.topology = {}
		
		for i in range(len(tmp_topology)):
			tmp_host = filter(lambda x: x.hostId == tmp_topology[i].hostId, handler_cm_api.cluster_hosts)[0]
			self.topology[tmp_topology[i].hostId] = tmp_host.hostname

	def get_current_group(self):
		return self._user_executing

###############################
# For internal validations

        def __validate_service(self, p_service):
                v_service = filter(lambda x: x.type == p_service, self.services)

                if not v_service:
                        print("Error: Service not found")
                        raise SystemExit

                return v_service.pop()

        def __validate_hostname(self, p_hostname):
                v_node = filter(lambda x: x.hostname == p_hostname, handler_cm_api.cluster_hosts)
                if not v_node:
                        print("Error: Hostname not found")
                        raise SystemExit

                return v_node.pop()

        def __validate_role(self, p_service, p_role, p_hostname):
                v_service = self.__validate_service(p_service)
                v_node = self.__validate_hostname(p_hostname)
                v_roles = filter(lambda x: x.type == p_role, v_service.get_all_roles())
                v_role = filter(lambda x: x.hostRef.hostId == v_node.hostId, v_roles)

                if not v_role:
                        print("Error: Role not found in that host")
                        raise SystemExit

                return v_role.pop()

######################################################################
# START/STOP/RESTART
######################################################################
	def stop_cluster(self):
		v_cmd = self.cluster.stop()
		v_msg = f_waiting_task(v_cmd)
		print(coloring(*v_msg))

	def start_cluster(self):
		v_cmd = self.cluster.start()
		v_msg = f_waiting_task(v_cmd)
		print(coloring(*v_msg))

	def restart_cluster(self):
                v_cmd = self.cluster.restart()
                v_msg = f_waiting_task(v_cmd)
                print(coloring(*v_msg))		

	def rolling_restart_cluster(self):
                v_cmd = self.cluster.rolling_restart()
                v_msg = f_waiting_task(v_cmd)
                print(coloring(*v_msg))

######################################################################
#SERVICES
######################################################################
################
# Status
################
# ------ State
	def check_state_services(self):
		for v_srv in self.services:	
			print(coloring(v_srv.serviceState, v_srv.type))


	def check_state_service(self, p_service):
		v_service = self.__validate_service(p_service)
		print(coloring(v_service.serviceState, v_service.type))


	def check_health_services(self):
		for v_srv in self.services:
			print(coloring(v_srv.healthSummary, v_srv.type))

# ----- Health
	def check_health_service(self, p_service):
		v_service = self.__service_validate(p_service)
		print(coloring(v_service.healthSummary, v_service.type))

#####################################
# stop/start/restart/Rolling Restart
#####################################
        def stop_service(self, p_service):
                v_service = self.__validate_service(p_service)
                print("* Stopping " + v_service.type)
                v_cmd = v_service.stop()
                v_msg = f_waiting_task(v_cmd)
                print(coloring(*v_msg))


        def start_service(self, p_service):
                v_service = self.__validate_service(p_service)
                print("* Starting " + v_service.type)
                v_cmd = v_service.start()
                v_msg = f_waiting_task(v_cmd)
                print(coloring(*v_msg))

        def restart_service(self, p_service):
                v_service = self.__validate_service(p_service)
                print("* Restarting " + v_service.type)
                v_cmd = v_service.restart()
                v_msg = f_waiting_task(v_cmd)
                print(coloring(*v_msg))

        def rolling_restart_service(self, p_service):
                v_service = self.__validate_service(p_service)
                try:
                        print(" * Rolling Restarting " + v_service.type)
                        v_cmd = v_service.rolling_restart()
                        v_msg = f_waiting_task(v_cmd)
                        print(coloring(*v_msg))
                except:
                        if re.match("Command not valid for", str(sys.exc_info()[1])):
                                print "It's not possible to use Rolling Restart in this service."
                        else:   raise


###################################################################
# ROLES
###################################################################
#################
# Status
#################

# ---- State
	def check_state_roles(self, p_service):
		v_service = self.__validate_service(p_service)
		print("*" + v_service.type + ":")
		for v_role in v_services.get_all_roles():
               		print(coloring(v_role.roleState, filter(lambda x: x.hostId==v_role.hostRef.hostId, handler_cm_api.cluster_hosts)[0].hostname) +":\t" +v_role.type)


	def check_state_role(self, p_service, p_role):
                v_service = self.__validate_service(p_service)
       		print("*" + v_service.type + ":")
            	v_roles = filter(lambda x: x.type == p_role, v_service.get_all_roles())
       	    	for v_role in v_roles:       
       	    		print(coloring(v_role.roleState, filter(lambda x: x.hostId==v_role.hostRef.hostId, handler_cm_api.cluster_hosts)[0].hostname) + ":\t" + v_role.type)

        def check_state_all_roles(self):
                for v_service in self.services:
                        self.check_state_roles(v_service.type)
                        print('---------------------')

# ---- Health
	def check_health_roles(self, p_service):
                v_service = self.__validate_service(p_service)
		print("*" + v_service.type + ":")
		for v_role in v_service.get_all_roles():
			print(coloring(v_role.healthSummary, filter(lambda x: x.hostId==v_role.hostRef.hostId, handler_cm_api.cluster_hosts)[0].hostname) +":\t" +v_role.type)


	def check_health_role(self, p_service, p_role):
                v_service = self.__validate_service(p_service)
	        print("*" + v_service.type + ":")
            	v_roles = filter(lambda x: x.type == p_role, v_service.get_all_roles())
            	for v_role in v_roles:       
	            	print(coloring(v_role.healthSummary, filter(lambda x: x.hostId==v_role.hostRef.hostId, handler_cm_api.cluster_hosts)[0].hostname) + ":\t" + v_role.type)

	def check_health_all_roles(self):
		for v_service in self.services:
			self.check_health_roles(v_service.type)
			print('---------------------')

#####################
# Stop/Start/Restart

        def stop_role(self, p_service, p_role, p_hostname):
                v_service = self.__validate_service(p_service)
                v_node = self.__validate_hostname(p_hostname)
                v_role = self.__validate_role(p_service, p_role, p_hostname)

                print("* Stopping " + v_role.type)
                v_cmd = v_service.stop_roles(v_role.name)
                v_msg = f_waiting_task(v_cmd[0])
                print(coloring(*v_msg))

        def start_role(self, p_service, p_role, p_hostname):
                v_service = self.__validate_service(p_service)
                v_node = self.__validate_hostname(p_hostname)
                v_role = self.__validate_role(p_service, p_role, p_hostname)

                print("* Starting " + v_role.type)
                v_cmd = v_service.start_roles(v_role.name)
                v_msg = f_waiting_task(v_cmd[0])
                print(coloring(*v_msg))


        def restart_role(self, p_service, p_role, p_hostname):
                v_service = self.__validate_service(p_service)
                v_node = self.__validate_hostname(p_hostname)
                v_role = self.__validate_role(p_service, p_role, p_hostname)

                print("* restarting " + v_role.type)
                v_cmd = v_service.restart_roles(v_role.name)
                v_msg = f_waiting_task(v_cmd[0])
                print(coloring(*v_msg))

###########################################################
#IMPALA QUERIES
###########################################################
# FILTERS
############################
	def setup_filters_impala_queries(self):
		v_start_time = raw_input('Introduce the start time with following format: DD/MM/YYYY_hh:mm:ss. Example: 01/01/2018_00:00:00: ')
		if not re.match("^\d{2}/\d{2}/20\d{2}_\d{2}:\d{2}:\d{2}$", v_start_time):
			print("Error: Invalid Format for start time")
			return

		v_end_time = raw_input('Introduce the end time with the following format: DD/MM/YYYY_hh:mm:ss. Example 31/01/2018_00:00:00: ')
		if not re.match("^\d{2}/\d{2}/20\d{2}_\d{2}:\d{2}:\d{2}$", v_end_time):
			print("Error: Invalid format for end time")
			return

		v_filter_type = raw_input('Choose the kind of filter: user|duration|state: ')
		if not v_filter_type in('user', 'duration', 'state'):
			print("Error: Invalid kind of filter")
			return

		if v_filter_type == 'user':
			v_filter_value = raw_input('Introduce the user name you want to filter by: ')
			if not v_filter_value:
				print("Error: Invalid user name")
				return

		elif v_filter_type == 'duration':
			v_filter_value = raw_input('Introduce the query duration you want to filter by: +Xs|-Xs|=Xs. Example: +0s: ')
			if not re.match("^[+-=]\d+.\d*[hms]$", v_filter_value):
				print("Error: Invalid duration filter.")
				return

		elif v_filter_type == 'state':
			v_filter_value = raw_input('Introduce the query state you want to filter by: CREATED|INITIALIZED|COMPILED|RUNNING|FINISHED|EXCEPTION|UNKNOWN: ')
			if not v_filter_value in ('CREATED', 'INITIALIZED', 'COMPILED', 'RUNNING', 'FINISHED', 'EXCEPTION', 'UNKNOWN'):
				print("Error: Invalid state filter.")
				return

		v_limit = raw_input("Introduce the max num of queries you want to check: ")
		if not re.match("^\d+$", v_limit):
			print("Error: Invalid limit. It has to be an integer")
			return

		return v_start_time, v_end_time, v_filter_type, v_filter_value, int(v_limit)
			
######################################
# Getting queries
######################################
	def get_impala_queries(self, p_start_time=None, p_end_time=None, p_filter_type=None, p_filter_value=None, p_limit=None):
		if not(p_start_time and p_end_time and p_filter_type and p_filter_value and p_limit):
			p_start_time, p_end_time, p_filter_type, p_filter_value, p_limit = self.setup_filters_impala_queries()


		v_impala = filter(lambda x: x.type == 'IMPALA', self.services)[0]

		if not v_impala:
			print("Error: Impala service doesnt exist in this cluster.")
			return

		if re.match("^\d{2}/\d{2}/20\d{2}_\d{2}:\d{2}:\d{2}$", p_start_time):	v_start_time = datetime.strptime(p_start_time, '%d/%m/%Y_%H:%M:%S')
		else:
			print("Error. startTime format is not valid.")
			return

		if re.match("^\d{2}/\d{2}/20\d{2}_\d{2}:\d{2}:\d{2}$", p_start_time):	v_end_time = datetime.strptime(p_end_time, '%d/%m/%Y_%H:%M:%S')
		else:
			print("Error. startTime format is not valid.")
			return	

		if p_filter_type == "user" and type(p_filter_value) == str:		v_filter_str = 'user = ' + p_filter_value
		elif p_filter_type == "duration" and re.match("^[+-=]\d+.\d*[hms]$", p_filter_value):
			if p_filter_value[0] == '+':	v_filter_value = p_filter_value.replace('+', '>')
			if p_filter_value[0] == '-':	v_filter_value = p_filter_value.replace('-', '<')
			v_filter_str = 'queryDuration ' + v_filter_value

		elif p_filter_type == "state" and p_filter_value in ('CREATED', 'INITIALIZED', 'COMPILED', 'RUNNING', 'FINISHED', 'EXCEPTION', 'UNKNOWN'):
			v_filter_str = 'queryState = ' + v_filter_value

		else:
			print("Error: Filter is not valid.")
			return

		if type(p_limit) == int and p_limit < 201:	v_limit = p_limit
		else:
			print("Error: Limit is not valid. It must be > 0 and <= 200")
			return

		v_queries = v_impala.get_impala_queries(v_start_time, v_end_time, v_filter_str, v_limit).queries
	
		v_output = ''
		for vq in v_queries:
			v_coordinator = filter(lambda x: x.hostId == vq.coordinator.hostId, self.cluster_hosts)[0].hostname

			v_output += COLORS.BLUE + "##################################################################################" + COLORS.RESET + "\n"
			v_output += vq.queryId + " -- " + vq.queryState + ":\n"
			v_output += COLORS.RED + vq.statement + COLORS.RESET + "\n"
			v_output += COLORS.GREEN + "--- Attributes ---" + COLORS.RESET + "\n"
		        v_output += "Query Type: " + vq.queryType + "\n"
        		if 'query_status' in vq.attributes.keys():              v_output += "Query Status: " + vq.attributes['query_status'] +"\n"

			v_output += "User: " + vq.user + "\n"
			v_output += "Database: " + vq.database + "\n"
	        	if 'pool' in vq.attributes.keys():                      v_output += "Pool: " + vq.attributes['pool'] +"\n"

	        	v_output += "Starts at: " + vq.startTime.strftime("%d/%m/%Y_%H:%M:%S") + "\n"
		        v_output += "Ends at: " + vq.endTime.strftime("%d/%m/%Y_%H:%M:%S") + "\n"
        		v_output += "Coordinator: " + v_coordinator + "\n"
		        v_output += "Rows Produced: " + str(vq.rowsProduced) + "\n"

			if vq.attributes['file_formats']:			v_output += "File Format: " + vq.attributes['file_formats'] +"\n"
			if 'hdfs_bytes_read' in vq.attributes.keys():		v_output += "HDFS bytes read: " + vq.attributes['hdfs_bytes_read'] + "\n"
			if 'memory_aggregate_peak' in vq.attributes.keys():	v_output += "Memory Aggregate Peak: " + vq.attributes['memory_aggregate_peak'] + "\n"
			if 'thread_cpu_time' in vq.attributes.keys():		v_output += "Threads Cpu Time: " + vq.attributes['thread_cpu_time'] + "\n"

		print(v_output)
		print("Do you want to save the output? (Y/N)")
		v_save = raw_input("Your choice: ").upper()
		if v_save == 'Y':
			v_output_nc = re.sub("\\x1b\[\d+m", "", v_output)
			v_file = "/tmp/impala_queries_" + datetime.now().strftime("%Y%m%d_%H%M%S")+".log"
			with open(v_file, 'a') as file_output:
				file_output.write(v_output_nc)
                        print("The output was written in: " + v_file)


######################
# Getting details
######################
	def get_details_impala_query(self, p_query_id=None):
		if not p_query_id:
			v_query_id = raw_input('Introduce the query id you want to check the details: ')
		else:
			v_query_id = p_query_id

		v_impala = filter(lambda x: x.type == 'IMPALA', self.services)[0]
		v_queries = v_impala.get_impala_queries(datetime.now()-timedelta(days=30) , datetime.now(), 'queryDuration > 0s', 1000).queries
	
		v_query = filter(lambda x: x.queryId == v_query_id, v_queries)
		if not v_query:
			print("Error: The query_id is not valid, was executed more than 30 days ago or is not between the last 1000 queries. 1000 is the limit.")
			return
		elif not v_query[0].detailsAvailable:
			print("Error: This Query does not have details available.")
			return
		else:
			v_output = "/tmp/impala_query_details_" + v_query[0].queryId + "_" + datetime.now().strftime("%Y%m%d_%H%M%S")+".log"
			with open(v_output, 'a') as file_output:
				file_output.write(str(v_impala.get_query_details(v_query[0].queryId)))
			print("The output was written in: " + v_output)	


#######################
	def get_same_configuration(self):
		v_configs = []
		v_command = 'hadoop org.apache.hadoop.conf.Configuration'

		for v_node in self.topology.values():
			v_ssh = subprocess.Popen(["ssh", v_node, "-o", "StrictHostKeyChecking=no", v_command], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
			v_configs += [v_ssh.stdout.readlines()]

		if len(self.topology) != len(v_configs):
			print("Error: The num configs is different to the num of nodes in this cluster")
			return

		if v_configs[1:] == v_configs[:-1]:
			print(coloring('GOOD', "The configs are the same in all nodes."))
			print("The nodes which were checked are: " + ', '.join(self.topology.values()))

		else:
			print(coloring('BAD', "The configs are not the same."))
