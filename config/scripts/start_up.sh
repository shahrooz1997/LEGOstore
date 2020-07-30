#!/usr/bin/env bash

# Chooses the first project Id in the list
#project_id=$(gcloud projects list | awk '{if(NR==2) id=$1} END {print id}')

# Chooses the deafult project
project_id=$(gcloud config list | awk '/^project.*/ {print $3}')

num_servers=$1
num_clients=$2
max_socket=4096
max_open_file=4096

#IMPORTANT
USER="namitha_snambiar95"
CLIENT_BASE_PORT=15001
SERVER_BASE_PORT=10000

if [ -z $num_servers ]
then
	echo "Please specify the following arguments :"
	echo "<num_of_servers> <num_of_clients> <create_controller_instance><server_zones_list><controller_zone>"
	exit 1
fi

function create_server_list(){

	servers=$(gcloud compute instances list --format="value(name, zone, EXTERNAL_IP)" --filter="name ~ ^data-server.*" --quiet)
	
	local name
	local zone
	local ip	

	i=0
	while read name zone ip 
	do
		names[$i]=$name
		zones[$i]=$zone
		ips[$i]=$ip

		echo "server info is ${names[$i]}"
		(( i++ ))
	done <<< "$servers"
}


function deploy_servers(){
	echo "The number of server instances to be created is $num_servers"

	i=0
	local server_zones=$1

	local disk_size=10GB
	local cpu_cores=1
	local cpu_memory=4GB

	while read zone 
	do
		#Break when requisite number of servers are already started
		if (( i >= num_servers))
		then
			break
		fi

		echo "Starting the server number $i and zone: $zone"
		gcloud compute  --project=$project_id instances create data-server-$i --zone=$zone --custom-cpu=$cpu_cores\
		--custom-memory=$cpu_memory --image-family=ubuntu --boot-disk-size=$disk_size\
		--boot-disk-type=pd-standard --no-shielded-secure-boot --shielded-vtpm --shielded-integrity-monitoring 
	
		((i++))
	done < "$server_zones"

	echo "All the servers instances are up" 

	if (( i < num_servers))
	then
		echo "Only $i servers were started, because of limited zones specified"
	fi

}

function generate_setup_config(){
	
	# Create setup_config.json
	
	i=0 	#idx of array
	input_file="local_config.json"
	contents=$(jq '.' $input_file)

	local PORT=$SERVER_BASE_PORT
	echo "Read the input json"
	while [ $i -lt ${#names[*]} ]
	do
		echo " the addr is ${ips[$i]} for $i client"
		contents=$(echo $contents | jq --arg idx "$((i+1))" --arg ipaddr "${ips[$i]}" ' .[$idx].servers."1".host = $ipaddr')
		contents=$(echo $contents | jq --arg idx "$((i+1))" --arg p "$PORT" ' .[$idx].servers."1".port = $p')
		(( i++)) 
		(( PORT++ ))	
	done
	
	echo "Dumpting values to the file"
	echo $contents | jq '.' > "setup_config.json"
	

}
# runs the application on all the server intances that are running
# Doesn't check num_servers - only used while creating instances
function run_servers(){
	
	i=0
	local PORT=$SERVER_BASE_PORT
	while [ $i -lt ${#names[*]} ]
	do
		local output=$(gcloud compute scp ./server_startup.sh $USER@${names[$i]}:~/LEGOstore/config/scripts/ --zone=${zones[$i]})

		output=$(gcloud compute ssh $USER@${names[$i]} --zone=${zones[$i]}\
			 --command="screen -S server_$i -d -m ~/LEGOstore/config/scripts/server_startup.sh $max_socket $max_open_file $PORT db_$i")
		echo $output
		echo "satrting server startup in $i"
	
		(( i++ ))
		(( PORT++ ))
	done	
	
	echo "Servers are running now!"	
}

function run_clients(){
	i=0
	# Delete old deployment is present	
	old_file=$(find . -name "deployment.txt")
	if [ ! -z $old_file ]
	then 
		rm $old_file
	fi  

	i=0
	local PORT=$CLIENT_BASE_PORT
	# Create deployment.txt and start client applications on the servers
	for addr in ${ips[*]} 
	do
		# Break when required number of clients already populated
		if (( i >= num_clients ))
		then 
			break
		fi
		echo "$addr:$PORT" >> "deployment.txt"
		local output=$(gcloud compute scp ./client_startup.sh $USER@${names[$i]}:~/LEGOstore/config/scripts/client_startup.sh --zone=${zones[$i]})
		output=$(gcloud compute ssh $USER@${names[$i]} --zone=${zones[$i]}\
			 --command="screen -S client_$i -d -m ~/LEGOstore/config/scripts/client_startup.sh $max_socket $max_open_file $PORT")
		
		((PORT++))
		(( i++ ))
	done
	
	echo "Finished client configuration and Deployment"

}

function deploy_controller(){

	local disk_size=10GB
	local cpu_cores=1
	local cpu_memory=3GB
	
	local create_instance=$1

	if [[ $2 =~ ^[0-9]+$ ]]
  	then
		local cnt=0
		for z in $(gcloud compute zones list --format="value(name)")
		do
			if [ $cnt -eq $2 ]
			then
				zone=$z	
			fi
			(( cnt++ ))	
		done
	else
		zone=$2
	fi

	local name="controller"
	
	echo "Starting the Controller and zone is : $zone"

	if [ $create_instance -ne 0 ]
	then
		gcloud compute  --project=$project_id instances create $name --zone=$zone --custom-cpu=$cpu_cores\
		--custom-memory=$cpu_memory --image-family=ubuntu --boot-disk-size=$disk_size\
		--boot-disk-type=pd-standard --no-shielded-secure-boot --shielded-vtpm --shielded-integrity-monitoring
	fi

	local output=$(gcloud compute scp ./deployment.txt ./setup_config.json ../input_workload.json $USER@$name:~/LEGOstore/config/ --zone=$zone)
	echo $output
	output=	$(gcloud compute scp ./controller_startup.sh $USER@$name:~/LEGOstore/config/scripts/ --zone=$zone)
	
	echo "Transferred the latest configuration file to the deployed controller"

	output=$(gcloud compute ssh $USER@$name --zone=${zone}\
		 --command="screen -S $name -d -m ~/LEGOstore/config/scripts/controller_startup.sh $max_socket $max_open_file")
	
	echo "Controller is running now!"	

}

#### main() ####

deploy_servers "$4"
create_server_list
generate_setup_config
run_servers
run_clients
deploy_controller $3 $5

exit 0
