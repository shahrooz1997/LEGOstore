import boto3
import os

def create_instance(region_name, image_id, instance_type='t2.micro'):
    client = boto3.resource('ec2', region_name=region_name)
    response = client.create_instances(ImageId=image_id,\
    MinCount=1,\
    MaxCount=1,\
    
    InstanceType=instance_type)
    return response

def create_instance_with_keypair(region_name, image_id, keypair, instance_type='t2.micro', size =50, DryRun=False):
    client = boto3.resource('ec2', region_name=region_name)
    response = client.create_instances(ImageId=image_id,
    BlockDeviceMappings=[
        {
            'DeviceName': '/dev/sda1',
            'Ebs': {
                'DeleteOnTermination': True,
                'VolumeSize': size,
                'VolumeType':'gp2',
            }
        }
    ],
    SecurityGroups=[
    'geostore',
    ],
    MinCount=1,
    MaxCount=1,
    KeyName=keypair, 
    DryRun=DryRun,
    InstanceType=instance_type)
    return response
    




def create_keypair(region_name, keyfilename, location, DryRun=False):
    client = boto3.resource('ec2', region_name=region_name)
    outfile = open(keyfilename, 'w')
    key_pair = client.create_key_pair(KeyName=location, DryRun=DryRun)
    key_pair_out = str(key_pair.key_material)
    outfile.write(key_pair_out)
    outfile.close()
    os.system("chmod 400 "+keyfilename)
    return key_pair



def start_instance(instance_id, region_name):
    client = boto3.client('ec2', region_name=region_name)
    responses = client.start_instances(InstanceIds=[instance_id])
    return responses


def stop_instance(instance_id, region_name):
    client = boto3.client('ec2', region_name=region_name)
    responses = client.stop_instances(InstanceIds=[instance_id])
    return responses

def terminate_instance(instance_ids, region_name):
    client = boto3.resource('ec2', region_name=region_name)
    if type(instance_ids) is not list:
        instance_ids = [instance_ids]
    responses = client.terminate_instances(InstanceIds=instance_ids)
    return responses

def send_command(cmds, instance_ids, region_name):
    
    '''
    try:
        # Here 'ubuntu' is user name and 'instance_ip' is public IP of EC2
        client.connect(hostname='18.221.239.117', username="ubuntu", pkey=key)


        cmd = "touch thisisfromboto3.txt"

        # Execute a command(cmd) after connecting/ssh to an instance
        stdin, stdout, stderr = client.exec_command(cmd)
        print(stdout.read())

        # close the client connection once the job is done
        client.close()

    except Exception as e:
        print(e)
    '''
    #XXX: try this, if not use top
    ssm_client = boto3.client('ssm', region_name=region_name)
    response = ssm_client.send_command(
                InstanceIds=instance_ids,
                DocumentName="AWS-RunShellScript",
                Parameters={'commands': cmds}, )
    
    command_id = response['Command']['CommandId']
    print(command_id)
    for instanceid in instanceids:
        output = ssm_client.get_command_invocation(
                 CommandId=command_id,
                 InstanceId=instanceid,
                 )
        print("output: ", output)
        #TODO: write into an S3 bucket






def print_options():
    
    print(">> What do you want to do?")
    print("\t1. send command to set")
    print("\t3. launch new set") 
    print("\t4. print aws regions info") 
    print("\t2. print servers info for set")
    pass

       
