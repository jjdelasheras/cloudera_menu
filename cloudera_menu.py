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

from handler_cm_api import handler_cm_api
import base64 
import json 
import os 
import getpass 
import grp 
import inspect 
import re
import subprocess

def user_privileges(p_privileges):
	v_role_user = []
	v_groups = subprocess.check_output(['groups']).split()
	if set(map(str, p_privileges['admin'])) & set(v_groups):	v_role_user += ['admin']
	if set(map(str, p_privileges['dev'])) & set(v_groups):  v_role_user += ['dev']
	if set(map(str, p_privileges['operator'])) & set(v_groups):  v_role_user += ['operator']

	return v_role_user

def validate_input(p_input):
	try:
		return int(p_input)
	except ValueError:
		pass	

def f_get_params(p_cluster, p_func):
	v_params = []
	if re.match('^((check)|(start)|(stop)|(restart)|(rolling_restart))_.*service$', p_func):
		os.system('clear')
		print("Choose the service name you want to work with:")
		for i in range(len(p_cluster.services)):
			print(str(i + 1) + ".- " + p_cluster.services[i].type) 
		
		v_service_inp = input("\nYour choose: ")
		if v_service_inp in range(1, len(p_cluster.services) + 1):
			v_params += [p_cluster.services[v_service_inp - 1].type]

	elif re.match('^((check)|(start)|(stop)|(restart))_.*role$', p_func):
		os.system('clear')
		print("Choose the service name you want to work with")
                for i in range(len(p_cluster.services)):
                        print(str(i + 1) + ".- " + p_cluster.services[i].type)

                v_service_inp = input("\nYour choose: ")
                if v_service_inp in range(1, len(p_cluster.services) + 1):
                        v_params += [p_cluster.services[v_service_inp - 1].type]

		os.system('clear')
		print("Choose the role you want to work with:")
		v_roles = p_cluster.services[v_service_inp - 1].get_all_roles()
	
		for i in range(len(v_roles)):
			print(str(i + 1) + ".- " + p_cluster.topology[v_roles[i].hostRef.hostId] + ":\t" + v_roles[i].type)

		v_role_inp = input("\nYour choose: ")
		if v_role_inp in range(1, len(v_roles) + 1):
			v_params += [v_roles[v_role_inp - 1].type, p_cluster.topology[v_roles[v_role_inp -1].hostRef.hostId]]
			
	print v_params
	return v_params


def f_choice(p_cluster, role_user):
	v_options = []
	v_options += [['For GETTING impala queries', 'get_impala_queries', ['dev', 'admin']]]
	v_options += [['For GETTING details from an impala query', 'get_details_impala_query', ['dev','admin']]]
	v_options += [['For CHECKING the services health', 'check_health_services', ['admin','operator']]]
	v_options += [['For CHECKING the roles health', 'check_health_all_roles', ['admin','operator']]]
	v_options += [['For STARTING a service', 'start_service', ['admin','operator']]]
	v_options += [['For STOPPING a service', 'stop_service', ['admin','operator']]]
	v_options += [['For RESTARTING a service', 'restart_service', ['admin','operator']]]
	v_options += [['For ROLLING RESTARTING a service', 'rolling_restart_service', ['admin','operator']]]
	v_options += [['For STARTING a ROLE', 'start_role', ['admin','operator']]]
	v_options += [['For STOPPING a ROLE', 'stop_role', ['admin','operator']]]
	v_options += [['For RESTARTING a role', 'restart_role', ['admin','operator']]]
	v_options += [['For getting the whole configuration matches', 'get_same_configuration', ['admin']]]

	v_operation = filter(lambda x: set(role_user) & set(x[2]), v_options)
	a = v_options[11]
	
	while 1:
		os.system('clear')
		print("Please Take a choice:")
		for i in range(len(v_operation)):
			print(str(i+1) + ".- " + v_operation[i][0])

		print('\nB.- For going back.')
		print('Q.- For leaving.')

		v_choice = raw_input('\nYour choice: ').upper()

		if v_choice == 'Q':
			os.system('clear')
			os._exit(1)
		if v_choice == 'B':	return

		v_choice = validate_input(v_choice)
		if not v_choice in range(1, len(v_operation) + 1):
			print("\nError: invalid choice")
			raw_input("Push ENTER to try again.")	

		else:
			func = v_operation[v_choice - 1][1]
			v_params = f_get_params(p_cluster, func)
			os.system('clear')
			getattr(p_cluster, func)(*v_params)
			raw_input("Push ENTER to continue")

######################################################################
# MENU
#####################################################################
def menu_environment(v_environment_list):
	while 1:
		os.system('clear')
		print("Please choose the environment:")
		for i in range(len(v_environment_list)):
			print(str(i+1) + ".- " + v_environment_list.keys()[i])

		print('\nQ.- For leaving')

		v_input_env = raw_input('\nYour choice: ').upper()
		if v_input_env == 'Q':	return

		v_input_env = validate_input(v_input_env)
		if not v_input_env in (range(1, len(v_environment_list) + 1)):
			print("\nError: Please, choose a valid environment.")
			raw_input("Push ENTER to try again").upper()

		else:
			v_key = v_environment_list.keys()[v_input_env - 1]
			v_clusters = v_environment_list[v_key]
				
			menu_clusters(v_clusters)
	

def menu_clusters(v_clusters_list):
	while 1:
		os.system('clear')
		print("Please choose the cluster you want to work with:")
	
		for i in range(len(v_clusters_list)):
			print(str(i+1) + ".- " + v_clusters_list[i].name)

		print('\nB.- For going back') 
		print('Q.- For leaving')

		v_input_clus = raw_input('\nYour choice: ').upper()

		if v_input_clus == 'Q': 
			os.system('clear')
			os._exit(1)

		if v_input_clus == 'B':	return

		v_input_clus = validate_input(v_input_clus)
		if not v_input_clus in (range(1, len(v_clusters_list) + 1)):
			print("\nError: Please, choose a valid cluster.")
			raw_input("Push ENTER to try again.").upper()
		else:
			v_cluster = v_clusters_list[v_input_clus - 1]
			f_choice(v_cluster, ROLE)

################################################################
# MAIN
################################################################
def main():
	v_environment = {}
	v_config = open('/home/jjdelasheras/git/cloudera_menu/config/config_cm.json')
	v_json = json.load(v_config)
	v_config.close()

	for v_env in v_json['environment'].keys():
		v_environment[v_env]=[]
		v_branch_environment = v_json['environment'][v_env]
		for v_cm in v_branch_environment.keys():
			tmp_cm = v_branch_environment[v_cm]
			cm_host = tmp_cm['cm_host']
			cm_port = None if not tmp_cm['cm_port'] else tmp_cm['cm_port']
			cm_user = tmp_cm['cm_user']
			cm_pass = base64.b64decode(tmp_cm['cm_pass'])
			cm_vers = tmp_cm['cm_vers']
			cm_tls = tmp_cm['cm_tls']
			for i in range(len(tmp_cm['clusters'])):
				v_cluster_name = tmp_cm['clusters'][i]
				v_cluster = handler_cm_api()
				v_cluster.setup(cm_host, cm_user, cm_pass, cm_vers, v_cluster_name, cm_port, cm_tls)
				v_environment[v_env].append(v_cluster)

	global ROLE 
	ROLE = user_privileges(v_json['privileges'])
	menu_environment(v_environment)
	raw_input('Push ENTER to continue.')
	os.system('clear')


if __name__ == '__main__':
	main()
