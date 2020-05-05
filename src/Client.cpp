#include "Util.h"
#include <iostream>
#include "Data_Transfer.h"

int main(int argc, char* argv[]){

	if(argc != 2){
		std::cout<<"Incorrect number of arguments: Please specify the port number" << std::endl;
		return 1;
	}
	int newSocket = socket_setup(argv[1]);
	std::cout<< "Waiting for connection from Controller." <<std::endl;
	int sock = accept(newSocket, NULL, 0);
	close(newSocket);

	std::string recv_str;
	try{

		if(DataTransfer::recvMsg(sock, recv_str) != 1){
			std::cout<< "Client Config could not be received! " << std::endl;
			return 1;
		}

		Properties cc = DataTransfer::deserializePrp(recv_str);
		std::cout<< "client id " << cc.local_datacenter_id << " group " << cc.groups[0]->grp_id[1] << " arrival rate " << cc.groups[0]->grp_config[1]->arrival_rate
					<< "read ratio  " << cc.groups[0]->grp_config[1]->read_ratio << "keys" << cc.groups[0]->grp_config[1]->keys[0] << std::endl;
		std::cout<< "group " << cc.groups[1]->grp_id[1] << " arrival rate " << cc.groups[1]->grp_config[1]->arrival_rate
								<< "read ratio  " << cc.groups[1]->grp_config[1]->read_ratio <<  cc.groups[1]->grp_config[1]->keys[0] << std::endl;
	}
	catch(std::logic_error &e){
		std::cout<< "Exception caught! " << e.what() << std::endl;
	}


	return 0;

}
