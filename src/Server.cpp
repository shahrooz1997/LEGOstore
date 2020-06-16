
#include <thread>
#include "Data_Server.h"
#include "Data_Transfer.h"
#include <sys/ioctl.h>
//#include <linux/sockios.h>
std::atomic<bool> reconfig(false);
std::mutex rcfglock;
struct rcfgSet{
	std::condition_variable rcv;
	std::string highest_timestamp;
};
//Maps key to it's reconfiguration meta data
std::unordered_map<std::string, rcfgSet*> rcfgKeys;

bool key_match(std::string curr_key){
	return (rcfgKeys.find(curr_key) != rcfgKeys.end());
}

int key_add(std::string curr_key){
	if(rcfgKeys.find(curr_key) == rcfgKeys.end()){
		rcfgKeys[curr_key] = new rcfgSet;
		return 1;
	}
	else{
		std::cout << "Reconfiguration for the same key is already in progresss !!" << std::endl;
		//assert(0);
		return -1;
	}
}

void update_timestamp(std::string curr_key, std::string curr_timestamp){
	rcfgKeys[curr_key]->highest_timestamp = curr_timestamp;
}


void server_connection(int connection, DataServer &dataserver, int portid){

	std::string recvd;
	int result = DataTransfer::recvMsg(connection,recvd);
	if(result != 1){
		strVec msg{"failure","No data Found"};
		DataTransfer::sendMsg(connection, DataTransfer::serialize(msg));
		close(connection);
		return;
	}

	// if data.size > 3
	// Data[0] -> method_name
	// Data[1] -> key
	// Data[2] -> timestamp
    strVec data = DataTransfer::deserialize(recvd);
	std::cout << "New METHOD CALLED "<< data[0] << " The value is " << data[2] <<"server port is" << portid << std::endl;
	std::string &method = data[0];

	std::unique_lock<std::mutex> rlock(rcfglock);
	if(reconfig.load() && key_match(data[1])){
		if(method != "write_config" && method != "reconfig_finalize" && method != "finish_reconfig"){
			rcfgSet *config = rcfgKeys[data[1]];
			//Block the thread
			while(reconfig.load()) {
				config->rcv.wait(rlock);
			}

			//TODO:: modify the client side of put to send timestamp always in data[2]
			//Check which operations to service and which to reject
			if(data.size() > 3 && !Timestamp::compare_timestamp(data[2], config->highest_timestamp)){
				//if data[2] =< highest, then service the thread
			}else{
				//Terminate the thread and send new config
				//TODO:: what is the config format
				result = DataTransfer::sendMsg(connection, std::string());
			}
		}
	}

	if(method == "put"){
		result = DataTransfer::sendMsg(connection, dataserver.put(data[1], data[3], data[2], data[4]));
	}else if(method == "get"){
		//std::cout << "GET fucntion called for server id "<< portid << std::endl;
		result = DataTransfer::sendMsg(connection, dataserver.get(data[1], data[2], data[3]));
	}else if(method == "get_timestamp"){
		result = DataTransfer::sendMsg(connection, dataserver.get_timestamp(data[1], data[2]));
	}else if(method == "put_fin"){
		result = DataTransfer::sendMsg(connection, dataserver.put_fin(data[1], data[2], data[3]));
	}else if(method == "reconfig_query"){
		reconfig = true;
		if(key_add(data[1]) == 1)
			result = DataTransfer::sendMsg(connection, dataserver.reconfig_query(data[1], data[2]));
	}else if(method == "reconfig_finalize"){
		update_timestamp(data[1], data[2]);
		result = DataTransfer::sendMsg(connection, dataserver.reconfig_finalize(data[1], data[2], data[3]));
	}else if(method == "write_config"){
		result = DataTransfer::sendMsg(connection, dataserver.write_config(data[1], data[3], data[2], data[4]));
	}else if(method == "finish_reconfig"){
		delete rcfgKeys[data[1]];
		rcfgKeys.erase(data[1]);
		reconfig = false;
	}
	else {
		DataTransfer::sendMsg(connection,  DataTransfer::serialize({"MethodNotFound", "Unknown method is called"}));
	}

	if(result != 1){
		DataTransfer::sendMsg(connection,  DataTransfer::serialize({"Failure","Server Response failed"}));
	}

	shutdown(connection, SHUT_WR);

	// //DEBUG_CAS_Server
	// int lastOutstanding = -1;
	// for(;;) {
	// 	int outstanding;
	// 	ioctl(connection, TIOCOUTQ, &outstanding);
	// 	if(outstanding != lastOutstanding)
	// 		printf("Outstanding: %d and socket id : %d\n", outstanding, connection);
	// 	lastOutstanding = outstanding;
	// 	if(!outstanding)
	// 		break;
	// 	std::this_thread::sleep_for(std::chrono::seconds(1));
	// }

	// int buffer = 0;
	// for(;;) {
	// 	int res=read(connection, &buffer, sizeof(buffer));
	// 	if(res < 0) {
	// 		perror("reading");
	// 		close(connection);
	// 		exit(1);
	// 	}
	// 	if(!res) {
	// 		printf("Correct EOF\n");
	// 		break;
	// 	}
	// }
	close(connection);
}

void test(DataServer *ds, int portid){

	while(1){
		std::cout<<"Alive port "<<portid<< std::endl;
		int new_sock = accept(ds->getSocketDesc(), NULL, 0);
		std::thread cThread([&ds, new_sock, portid](){ server_connection(new_sock, *ds, portid);});
		cThread.detach();
		std::cout<<"Received Request!!1  PORT:" <<portid<<std::endl;
	}

}

int main(){


	std::vector<std::string> socket_port{"10000", "10001", "10002", "10003","10004","10005","10006","10007","10008"};
	std::vector<std::string> db_list{"db1.temp", "db2.temp", "db3.temp", "db4.temp", "db5.temp", "db6.temp", "db7.temp", "db8.temp", "db9.temp"};

	DataServer *dsobj[9];
	for(int i=0; i<9 ;i++){
		dsobj[i] = new DataServer(db_list[i], socket_setup(socket_port[i]));
	}
	for(uint i=0; i< socket_port.size(); i++){
		//sobj[i])(db_list[i], socket_setup(socket_port[i]));
		//std::thread newServer([temp](int port){test(temp, port);}, i);
		std::thread newServer(test, dsobj[i], i);
		//std::cout<<"STarting new server at port " << socket_port[i] <<" and socket id " << temp.getSocketDesc() << std::endl;
		//std::thread newServer([&i,&socket_port,&db_list](){test(DataServer(db_list[i], socket_setup(socket_port[i])));});
		newServer.detach();
	}

	//TODO:: free dsobj object here or somewhere
	//TODO:: Change this, this is a temp FIX
	std::mutex lock;
	std::lock_guard<std::mutex> lck(lock);
	std::unique_lock<std::mutex> lckd(lock);
	return 0;
}
