
#include <thread>
#include "Data_Server.h"
#include "Data_Transfer.h"
#include <sys/ioctl.h>
//#include <linux/sockios.h>

void server_connection(int connection, DataServer &dataserver, int portid){

	std::string recvd;
	int result = DataTransfer::recvMsg(connection,recvd);
	if(result != 1){
		std::cout << "Waaaarninng!!!!!!!!! the result value is " << result << std::endl;
		strVec msg{"failure","No data Found"};
		DataTransfer::sendMsg(connection, DataTransfer::serialize(msg));
		close(connection);
		return;
	}

	//DEBUG -- check if receive queue is empty
	// char debug;
	// int res  = recv(connection, &debug, 1, 0);
	// assert(res == 0);


    strVec data = DataTransfer::deserialize(recvd);
	std::cout << "New METHOD CALLED "<< data[0] << " The value is " << data[2] <<"server port is" << portid << std::endl;
	std::string &method = data[0];
	if(method == "put"){
		result = DataTransfer::sendMsg(connection, dataserver.put(data[1], data[2], data[3], data[4]));
	}else if(method == "get"){
		//std::cout << "GET fucntion called for server id "<< portid << std::endl;
		result = DataTransfer::sendMsg(connection, dataserver.get(data[1], data[2], data[3]));
	}else if(method == "get_timestamp"){
		result = DataTransfer::sendMsg(connection, dataserver.get_timestamp(data[1], data[2]));
	}else if(method == "put_fin"){
		result = DataTransfer::sendMsg(connection, dataserver.put_fin(data[1], data[2], data[3]));
	}else {
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
