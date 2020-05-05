#include "Util.h"
#include <iostream>
#include "Data_Transfer.h"
#include <thread>
#include <chrono>
#include <algorithm>
#include <sys/wait.h>
#include <cmath>

std::atomic<bool> running(false);
//std::atomic<int> cnt (0);

int run_session(uint32_t obj_size, double read_ratio, std::vector<std::string> &key, Placement *pp){
	while(running.load()){
		std::cout << "Thread created !" << std::endl;
		//cnt+= 1;
		std::this_thread::sleep_for(std::chrono::seconds(10));
	}

}

int key_req_gen(Properties &prop, int grp_idx){

	using namespace std::chrono;
	using millis = duration<uint64_t, std::milli>;

	uint32_t clientId = prop.local_datacenter_id;
	uint64_t startTime = prop.start_time;
	time_point<system_clock, millis> timePoint(millis{startTime});


	std::cout<<"Waiting for the Start time...." << std::endl;
	std::this_thread::sleep_until(timePoint);
	std::cout << "Starting the request generation! " << std::endl;

	GroupConfig *grpCfg;
	for(int j=0; j<prop.groups.size() ; j++){
		// If the key id doesn't exist in this timestamp, skip it
		auto it = find(prop.groups[j]->grp_id.begin(), prop.groups[j]->grp_id.end(), grp_idx);
		if( it == prop.groups[j]->grp_id.end()){
			++j;
			continue;
		}

		// The config for this grp index
		grpCfg = prop.groups[j]->grp_config[it - prop.groups[j]->grp_id.begin()];

		running = true;
		//Start threads
		std::vector<std::thread> threads;
		long numReqs = lround(grpCfg->client_dist[clientId] * grpCfg->arrival_rate);

		for(long i=0; i<numReqs; i++){
			threads.emplace_back(run_session, grpCfg->object_size, grpCfg->read_ratio, std::ref(grpCfg->keys), grpCfg->placement_p);
		}

		std::cout<<"sleeeeeep...." << std::endl;
		timePoint += millis{grpCfg->duration* 1000};
		std::this_thread::sleep_until(timePoint);
		std::cout<<"yaaaaas...." << std::endl;

		running = false;
		for(auto &th: threads){
			th.join();
		}

		//std::cout<<"total threads created  : " << cnt.load()<< std::endl;
		threads.clear();

	}
}

int main(int argc, char* argv[]){

	if(argc != 2){
		std::cout<<"Incorrect number of arguments: Please specify the port number" << std::endl;
		return 1;
	}
	int newSocket = socket_setup(argv[1]);
	std::cout<< "Waiting for connection from Controller." <<std::endl;
	std::cout<< "Atomic variable is lock free? " << running.is_lock_free() << " : another opinion : " << ATOMIC_BOOL_LOCK_FREE << std::endl;
	int sock = accept(newSocket, NULL, 0);
	close(newSocket);

	std::string recv_str;
	try{

		if(DataTransfer::recvMsg(sock, recv_str) != 1){
			std::cout<< "Client Config could not be received! " << std::endl;
			return 1;
		}
		close(sock);

		Properties cc = DataTransfer::deserializePrp(recv_str);
		std::cout<< "client id " << cc.local_datacenter_id << " group " << cc.groups[0]->grp_id[1] << " arrival rate " << cc.groups[0]->grp_config[1]->arrival_rate
					<< "read ratio  " << cc.groups[0]->grp_config[1]->read_ratio << "keys" << cc.groups[0]->grp_config[1]->keys[0] << std::endl;
		std::cout<< "group " << cc.groups[1]->grp_id[1] << " arrival rate " << cc.groups[1]->grp_config[1]->arrival_rate
								<< "read ratio  " << cc.groups[1]->grp_config[1]->read_ratio <<  cc.groups[1]->grp_config[1]->keys[0] << std::endl;

		if(cc.groups.empty()){
			std::cout << "No Key groups were found" <<std::endl;
			return 0;
		}

		// Create a separate process for each Key group
		for(int i=0; i<cc.groups[0]->grp_id.size(); i++){
			if(fork() == 0){
				key_req_gen(cc, cc.groups[0]->grp_id[i]);
				exit(0);
			}
		}

		while(wait(NULL) >=0);
		//TOOD:: Free up the properties structure

	}
	catch(std::logic_error &e){
		std::cout<< "Exception caught! " << e.what() << std::endl;
	}


	return 0;

}
