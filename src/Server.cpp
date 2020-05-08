
#include <thread>
#include "Data_Server.h"
#include "Data_Transfer.h"

void server_connection(int connection, DataServer &dataserver, int portid){

	std::string recvd;
	int result = DataTransfer::recvMsg(connection,recvd);
	if(result != 1){
		valueVec msg{"failure","No data Found"};
		DataTransfer::sendMsg(connection, DataTransfer::serialize(msg));
		close(connection);
		return;
	}

    	valueVec data = DataTransfer::deserialize(recvd);
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
