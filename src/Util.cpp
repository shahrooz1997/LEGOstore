/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/*
 * File:   Util.cpp
 * Author: shahrooz
 *
 * Created on January 18, 2020, 5:18 PM
 */

  // for open/close pair
#include "Util.h"
#include <cstring>
#include <arpa/inet.h>


#ifdef DEVELOPMENT
bool DEBUG_CAS_Client = true;
bool DEBUG_ABD_Client = true;
bool DEBUG_CAS_Server = true;
bool DEBUG_ABD_Server = true;
bool DEBUG_RECONFIG_CONTROL = true;
#else
bool DEBUG_CAS_Client = false;
bool DEBUG_ABD_Client = false;
bool DEBUG_CAS_Server = false;
bool DEBUG_ABD_Server = false;
bool DEBUG_RECONFIG_CONTROL = false;
#endif


JSON_Reader::JSON_Reader() {
}

JSON_Reader::~JSON_Reader() {
}

void print_time(){
    auto timenow = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::cout << ctime(&timenow) << std::endl;

}
std::string convert_ip_to_string(uint32_t ip){
    ip = htonl(ip);
    unsigned char *p = (unsigned char*)(&ip);
    std::string ret;
    for(int i = 0; i < 4; i++){
        ret += std::to_string(*(p++));
        if(i != 3)
            ret += '.';
    }
    return ret;
}

// Used for creating server sockets
// Returns the socket FD after creation, binding and listening
// nullptr in IP implies the use of local IP
int socket_setup(const std::string &port, const std::string *IP){

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

            // enable = 0;
            // uint size_enable = sizeof(enable);
            // getsockopt(socketfd, SOL_SOCKET, SO_RCVBUF, &enable, &size_enable);
            // printf(" Size of the receive buffer is %u\n", enable);
            //
            //
            // enable = 212992;
            // if( setsockopt(socketfd, SOL_SOCKET, SO_RCVBUF, &enable, sizeof(enable)) == -1){
    		// 	perror("server -> set socket options");
    		// 	continue;
    		// }


        // struct linger sock_linger;
        // sock_linger.l_onoff = 1;
        // sock_linger.l_linger = MAX_LINGER_BEFORE_SOCK_CLOSE;
        // if( setsockopt(socketfd, SOL_SOCKET, SO_LINGER, &sock_linger, sizeof(sock_linger)) == -1){
        //     perror("server -> set socket options");
        //     continue;
        // }

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

//Returns 0 on success
int socket_cnt(int &sock, uint16_t port, const std::string &IP){

    struct sockaddr_in serv_addr;
    if((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0){
        perror("\n Socket creation error \n");
        return 1;
    }
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);
    std::string ip_str = IP;
    //std::string ip_str = convert_ip_to_string(server->ip);

    if(inet_pton(AF_INET, ip_str.c_str(), &serv_addr.sin_addr) <= 0){
        perror("\nInvalid address/ Address not supported \n");
        return 1;
    }

    if(connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0){
        perror("\nConnection Failed \n");
        return 1;
    }

    return 0;
}
