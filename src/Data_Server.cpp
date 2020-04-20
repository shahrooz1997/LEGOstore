#include "Cache.h"
#include "Persistent.h"
#include <thread>
#include "CAS_Server.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>	// for addrinfo struct def
#include <unistd.h>  // for open/close pair
#include "Data_Transfer.h"
#include <functional>

#define BACKLOG 10


class DataServer{

public:
	//TODO:: confirm the backlog value, set to 2048 in py
	// TODO:: removed the null value of socket, directly intializing class variable

	DataServer(std::string directory, int sock)
		: cache(500000000), persistent(directory), sockfd(sock){

	}

/*	DataServer(const DataServer &ds) {
		sockfd = ds.sockfd;
		cache(ds.cache);
		persistent(ds.persistent);
		CAS = ds.CAS;
	}*/
//	DataServer& operator = (const DataServer&) = delete;
	
	int getSocketDesc(){
		return sockfd;
	}

	valueVec get_timestamp(std::string &key, std::string &curr_class){

		if(curr_class == "CAS"){
			return CAS.get_timestamp(key,cache, persistent, lock);
		}else if(curr_class == "ABD"){

		}
	}

	valueVec put(std::string &key, std::string &value, std::string &timestamp, std::string &curr_class){
		if(curr_class == "CAS"){
			return CAS.put(key,value, timestamp, cache, persistent, lock);
		}else if(curr_class == "ABD"){

		}
	}

	valueVec put_fin(std::string &key, std::string &timestamp, std::string &curr_class){

		if(curr_class == "CAS"){
			return CAS.put_fin(key, timestamp, cache, persistent, lock);
		}else if(curr_class == "ABD"){

		}
	}

	valueVec get(std::string &key, std::string &timestamp, std::string &curr_class){
		
		if(curr_class == "CAS"){
			return CAS.get(key, timestamp, cache, persistent, lock);
		}else if(curr_class == "ABD"){

		}
	}
private:

	int sockfd;
	std::mutex lock;
	Cache cache;
	Persistent persistent;
	CAS_Server CAS;
};


// Returns the socket FD after creation, binding and listening
// nullptr in IP implies the use of local IP
int socket_setup(const std::string &port, const std::string *IP = nullptr){

	struct addrinfo hint, *res, *ptr;
	int status = 0, enable = 1;
	int socketfd;
	memset(&hint, 0, sizeof(hint));
	hint.ai_family = AF_INET;
	hint.ai_socktype = SOCK_STREAM;

	if(IP == nullptr){
		hint.ai_flags = AI_PASSIVE;
		status = getaddrinfo(NULL, port.c_str(), &hint, &res);
	}else{
		status = getaddrinfo((*IP).c_str(), port.c_str(), &hint, &res);
	}

	if(status != 0){
		fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(status));
		assert(0);
	}

	// Loop through all the options, unless one succeeds
	for( ptr = res; res != NULL ; ptr = ptr->ai_next){

		if((socketfd = socket(ptr->ai_family, ptr->ai_socktype, ptr->ai_protocol)) == -1){
			perror("server -> socket");
			continue;
		}

		if( setsockopt(socketfd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable)) == -1){
			perror("server -> set socket options");
			continue;
		}

		if( bind(socketfd, ptr->ai_addr, ptr->ai_addrlen) == -1){
			close(socketfd);
			perror("server -> bind");
			continue;
		}

		break;
	}

	freeaddrinfo(res);

	if(ptr == NULL){
		fprintf(stderr, "Socket failed to bind\n");
		assert(0);
	}

	if( listen(socketfd, BACKLOG) == -1){
		perror("server -> listen");
		assert(0);
	}

	return socketfd;
}

void server_connection(int connection, DataServer &dataserver, int portid){

	std::string recvd;
	int result = DataTransfer::recvMsg(connection,recvd);
    	valueVec data = DataTransfer::deserialize(recvd);


	if(result != 1){
		valueVec msg{"failure","No data Found"};
		DataTransfer::sendMsg(connection, DataTransfer::serialize(msg));
		//TODO:: Should I close connectoin here?
		return;
	}
	std::cout << "New METHOD CALLED "<< data[0] << " The value is " << data[2]
			<<"server port is" << portid << std::endl;
	std::string &method = data[0];
	if(method == "put"){
		result = DataTransfer::sendMsg(connection, DataTransfer::serialize(dataserver.put(data[1], data[2], data[3], data[4])));
	}else if(method == "get"){
		//std::cout << "GET fucntion called for server id "<< portid << std::endl;
		result = DataTransfer::sendMsg(connection, DataTransfer::serialize(dataserver.get(data[1], data[2], data[3])));
	}else if(method == "get_timestamp"){
		result = DataTransfer::sendMsg(connection, DataTransfer::serialize(dataserver.get_timestamp(data[1], data[2])));
	}else if(method == "put_fin"){
		result = DataTransfer::sendMsg(connection, DataTransfer::serialize(dataserver.put_fin(data[1], data[2], data[3])));
	}else {
		DataTransfer::sendMsg(connection,  DataTransfer::serialize({"MethodNotFound", "Unknown method is called"}));
	}

	if(result != 1){
		DataTransfer::sendMsg(connection,  DataTransfer::serialize({"Failure","Server Response failed"}));
	}
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
	for(int i=0; i< socket_port.size(); i++){
		//sobj[i])(db_list[i], socket_setup(socket_port[i]));
		//std::thread newServer([temp](int port){test(temp, port);}, i);
		std::thread newServer(test, dsobj[i], i);
		//std::cout<<"STarting new server at port " << socket_port[i] <<" and socket id " << temp.getSocketDesc() << std::endl;
		//std::thread newServer([&i,&socket_port,&db_list](){test(DataServer(db_list[i], socket_setup(socket_port[i])));});
		newServer.detach();
	}

	//TODO:: Change this, this is a temp FIX	
	std::mutex lock;
	std::lock_guard<std::mutex> lck(lock);
	std::unique_lock<std::mutex> lckd(lock);
	return 0;
}
 
