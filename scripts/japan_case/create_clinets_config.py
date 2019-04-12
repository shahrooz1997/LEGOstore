
import json
import os


if __name__ == '__main__':

	os.system("mkdir -p cli_configs/")

	client_config = json.load(open("client_config.json"))
	set_info = json.load(open("set1.json"))
	#japan_case = json.load(open("skewed_japan.json"))
	#groups = japan_case['input_groups']
	datacenter_config = client_config['datacenters']
	
	for i, location in enumerate(set_info.items()):
		client_id = i + 1
		datacenter_config[str(client_id)]['servers']["1"]["host"] = location[1]['instance_info']['public_ip']
		datacenter_config[str(client_id)]['servers']["1"]["port"] = "10000"

	#XXX: assume one group
	#TODO: don't hardcode
	keys = list(range(1000))
	groups = {"groups":{
					"g1" : {
						"keys": keys,
						"placement": {
							"protocol" : "CAS",
							"m"  : 5,
							"k"  : 1,
							"Q1" : ["9", "5", "6"],
							"Q2" : ["9", "5", "6"],
							"Q3" : ["9", "5", "6"],
							"Q4" : ["9", "5", "6"]}
						}
					}
			}
	
	_arrival_rate = 200
	client_distrib = [0.9, 0.025, 0.025, 0.0, 0.0, 0.0, 0.0, 0.025, 0.025]
	for i, distrib in enumerate(client_distrib):
		client_id = i + 1
		client_config['local_datacenter'] = str(client_id)
		client_config['datacenters']      = datacenter_config
		client_config['groups'] = groups
		client_config['arrival_rate'] = int(distrib * _arrival_rate)
		config_file = open("cli_configs/c{}.json".format(client_id), 'w')
		json.dump(client_config, config_file, indent=4)
		config_file.close()


