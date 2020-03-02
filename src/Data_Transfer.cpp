#include "Data_Transfer.h"


int DataTransfer::sendAll(int &sock, const void *data, int data_size){
	
	const char *iter = static_cast<const char*>(data);
	int bytes_sent = 0;
	
	while(data_size > 0){
		if( (bytes_sent = send(sock, iter, data_size, 0)) == -1){
			perror("sendAll -> send");
			return -1;
		}
		
		data_size -= bytes_sent;
		iter += bytes_sent;		
	}

	return 1;	
}

void DataTransfer::encode(const valueVec &data, std::string *out_str){
		
	packet::msg enc;
	for( auto it : data){	
		enc.add_value(it);
	}

	enc.SerializeToString(out_str);
}


//Called after socket connection established
//Serializes the msg and sends it across the socket
// Return 1 on SUCCESS
int DataTransfer::sendMsg(int &sock, const valueVec &data){
	
	std::string out_str;
	encode(data, &out_str);

	int32_t _size = htonl(out_str.size());
	
	int result = sendAll(sock, &_size, sizeof(_size));
	if(result == 1){
		result = sendAll(sock, out_str.c_str(), out_str.size());
	}
	
	return result;
}

int DataTransfer::recvAll(int &sock, void *buf, int data_size){
	
	char *iter = static_cast<char *>(buf);
	int bytes_read = 0, result; 
	

	if(data_size > 0){
		if( (bytes_read = recv(sock, iter, data_size, 0)) < 1){
			if(bytes_read == -1){
				perror("recvALL -> recv");
			}else if(bytes_read == 0){
				fprintf(stderr, "Recvall Failed : Connection disconnected\n ");
			}
			return bytes_read;			
		}

		iter += bytes_read;
		data_size -= bytes_read;
	}

	return 1;
}

void DataTransfer::decode(std::string &data, valueVec &out_data){

	packet::msg dec;
	dec.ParseFromString(data);
	
	int val_size  = dec.value_size();

	for(int i=0; i<val_size; i++){
		out_data.push_back(dec.value(i));
	}
}

//Pass an empty Vector to it
// Decodes the msg and populates the vector
//Returns 1 on SUCCESS
int DataTransfer::recvMsg(int sock, valueVec &in_data){
	
	int32_t _size;
	int result;
	std::string data;

	result = recvAll(sock, &_size, sizeof(_size));
	if(result == 1){
		
		_size = ntohl(_size);
		if(_size > 0){
			data.resize(_size);
			result = recvAll(sock, const_cast<char *>(data.c_str()), _size);
			if(result != 1){
				data.clear();
				return result;
			}
		}
	}
	
	decode(data, in_data);
	return 1;
}

