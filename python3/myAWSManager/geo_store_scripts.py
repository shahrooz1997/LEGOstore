
from utils import *
from pathlib import Path

### testing ###
import json
import os

###


class instance_info(object):
    def __init__(self, instance_id, public_ip):
        self.instance_id = instance_id
        self.public_ip = public_ip




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

    


def launch_set(regions,set_num, instance_type='t2.micro', DryRun = True):
    os.system("mkdir -p sets")
    filename = "sets/set{}.json".format(set_num)
    my_file = Path(filename)
    if my_file.is_file():
        print( filename, "file already exists!!")
        return
    set_info_file = open(filename, 'w')
    set_info = {}   
    for location in regions.keys():
        image_id = regions[location]['image_id'] 
        region_name = regions[location]['region_name']
        resp = create_instance_with_keypair(region_name,\
                                            image_id,\
                                            location,
                                            instance_type=instance_type,
                                            DryRun=DryRun) 
        set_info.update({region_name:{'instance_info':instance_info(resp[0].instance_id, resp[0].public_ip_address).__dict__}})

    json.dump(set_info, set_info_file, indent=4)
    set_info_file.close()

    return set_info


def stop_set(set_info):
    for location in set_info.items():
        region_name = location[0]
        instance_id = location[1]['instance_info']['instance_id']
        stop_instance(instance_id, region_name)
   
def start_set(set_info):
    for location in set_info.items():
        region_name = location[0]
        instance_id = location[1]['instance_info']['instance_id']
        start_instance(instance_id, region_name)

def update_public_ips(set_info, set_num):
    filename = "sets/set{}.json".format(set_num)
    for location in set_info.items():
        #print(location)
        region_name = location[0]
        instance_id = location[1]['instance_info']['instance_id']
        set_info[region_name]['instance_info']['public_ip'] = get_public_ips([instance_id], region_name)[0][1]
    set_info_file = open(filename, 'w')
    json.dump(set_info, set_info_file, indent=4)
    set_info_file.close()
    




def run_cmd_on_set(set_info):
    raise NotImplementedError
def terminate_set(set_info):
    #no need for now
    raise NotImplementedError
    


if __name__ == '__main__':
    #setup_aws_credentials('credentials.csv')
    regions = json.load(open("aws_regions.json"))
    set_info_1 = json.load(open("sets/set1.json"))
    set_info_2 = json.load(open("sets/set2.json"))
    
    #print(json.dumps(set_info, indent=4))
    #start_set(set_info)
    #update_public_ips(set_info, 2)
    #create_keypairs(regions)
    #launch_set(regions,2, instance_type='c5.xlarge', DryRun=False)
    #set_info = json.load(open("sets/set2.json"))

    stop_set(set_info_1)
    stop_set(set_info_2)
    



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

