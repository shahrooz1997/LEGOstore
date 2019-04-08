
from util import *


### testing ###
import json
import os

###

def setup_aws_credentials(filepath):
    credential_file = open(os.path.expanduser('~')+'/.aws/credentials', 'w')
    data_credential_file = "[default]\naws_access_key_id = {}\naws_secret_access_key = {}"
    with open(filepath, 'r') as fd:
        info = fd.readlines()[1].split(',')
        access_key = info[2]
        secret_key = info[3]
    credential_file.write(data_credential_file.format(access_key, secret_key))
    credential_file.close()
    print("Credentials updated!")
    
    

def create_keypairs(regions):
    os.system("mkdir -p keypairs")
    keypair_path = 'keypairs/'
    for location in regions.keys():
        region_name = regions[location]['region_name']
        filepath = keypair_path + location + '.pem'
        try:
            create_keypair(region_name, filepath, location)
        except Exception as e:
            print(location, ":",e)

    


def launch_set(regions,set_num, instance_type='t2.micro'):
    os.system("mkdir -p sets")
    filename = "sets/set{}.json".format(set_num)
    set_info_file = open(filename, 'w')
    set_info = {}   
    for location in regions.keys():
        image_id = regions[location]['image_id'] 
        region_name = regions[location]['region_name']
        resp = create_instance_with_keypair(region_name,\
                                            image_id,\
                                            location,
                                            instance_type=instance_type) 
        set_info.update({region_name:resp[0].instance_id})

    json.dump(set_info, set_info_file)
    set_info_file.close()

    return set_info


def stop_set(set_info):
    for location in set_info.items():
        region_name = location[0]
        instance_id = location[1]
        stop_instance(instance_id, region_name)
   
def start_set(set_info):
    for location in set_info.items():
        region_name = location[0]
        instance_id = location[1]
        start_instance(instance_id, region_name)

def get_public_ip(set_info):
    pass


def run_cmd_on_set(set_info):
    raise NotImplementedError
def terminate_set(set_info):
    #no need for now
    raise NotImplementedError
    


if __name__ == '__main__':
    #setup_aws_credentials('credentials.csv')
    regions = json.load(open("aws_regions.json"))
    set_info = json.load(open("sets/set1.json"))
    #create_keypairs(regions)
    #launch_set(regions,1, instance_type='t2.xlarge')
    #stop_set(set_info)
    start_set(set_info)
    



#### testing ####
#
#regions = json.load(open("test_region.json"))
#set_info = launch_set(regions)
#for resp in set_info.items():
#    print(resp[0],' >> instance ', resp[1].instance_id, 'launched!')
#
#input("press enter to terminate all instances above: ")
#
#
#for resp in set_info.items():
#    terminate_instance(resp[1].instance_id, resp[0])
#
#print("terminated instances!")

