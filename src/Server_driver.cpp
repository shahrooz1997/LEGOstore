#include <thread>
#include "../inc/Data_Server.h"
#include "../inc/Data_Transfer.h"
#include <sys/ioctl.h>
#include <unordered_set>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <fstream>
#include <netinet/tcp.h>
#include "../inc/Util.h"
#include <iostream>
#include <atomic>
#include <sys/wait.h>

using namespace std;

static vector<string> ports = {"10000", "10001", "10002", "10003", "10004", "10005", "10006", "10007", "10008"};
static vector<string>
    db_names = {"db1.temp", "db2.temp", "db3.temp", "db4.temp", "db5.temp", "db6.temp", "db7.temp", "db8.temp",
                "db9.temp"};

void message_handler(int connection, DataServer &dataserver, string &recvd) {
  DPRINTF(DEBUG_SERVER, "started\n");
  EASY_LOG_INIT_M(string("started"));
  int result = 1;
  unique_ptr<packet::Operation::MethodType> method_p;
  unique_ptr<vector<string>> data_p;
  unique_ptr<string> msg_p;
  unique_ptr<string> key_p;
  unique_ptr<string> timestamp_p;
  unique_ptr<string> value_p;
  unique_ptr<packet::Operation::AlgorithmName> algorithm_name_p;
  unique_ptr<uint32_t> conf_id_p;
  unique_ptr<uint32_t> new_conf_id_p;

  assert(DataTransfer::deserializeOperation(recvd,
                                            method_p,
                                            data_p,
                                            msg_p,
                                            key_p,
                                            timestamp_p,
                                            value_p,
                                            algorithm_name_p,
                                            conf_id_p,
                                            new_conf_id_p) == 0);

  const char *algorithm_name = getAlgorithmName(*algorithm_name_p);

  DPRINTF(DEBUG_SERVER,
          "%s\n",
          (string("method is ") + to_string(*method_p) + ", data size is " + to_string(data_p->size()) + ", msg is "
              + *msg_p + ", key is " + *key_p + ", ts is " + *timestamp_p + ", value is " + TRUNC_STR(*value_p)
              + ", class is " + algorithm_name
              + ", conf_id is " + to_string(*conf_id_p) + ", new_conf_id is " + to_string(*new_conf_id_p)).c_str());
  EASY_LOG_M(string("method is ") + to_string(*method_p) + ", data size is " + to_string(data_p->size()) + ", msg is "
                 + *msg_p + ", key is " + *key_p + ", ts is " + *timestamp_p + ", value is " + TRUNC_STR(*value_p)
                 + ", class is " + algorithm_name
                 + ", conf_id is " + to_string(*conf_id_p) + ", new_conf_id is " + to_string(*new_conf_id_p));

  switch (*method_p) {
    case packet::Operation_MethodType_put:
      result = DataTransfer::sendMsg(connection,
                                     dataserver.put(*key_p, *value_p, *timestamp_p, algorithm_name, *conf_id_p));
      break;
    case packet::Operation_MethodType_get:
      result = DataTransfer::sendMsg(connection,
                                     dataserver.get(*key_p,
                                                    *timestamp_p,
                                                    algorithm_name,
                                                    *conf_id_p));
      break;
    case packet::Operation_MethodType_get_timestamp:
      result = DataTransfer::sendMsg(connection,
                                     dataserver.get_timestamp(*key_p,
                                                              algorithm_name,
                                                              *conf_id_p));
      break;
    case packet::Operation_MethodType_put_fin:
      result =
          DataTransfer::sendMsg(connection, dataserver.put_fin(*key_p, *timestamp_p, algorithm_name, *conf_id_p));
      break;
    case packet::Operation_MethodType_reconfig_query:
#ifdef CHANGE_THREAD_PRIORITY
      increase_thread_priority();
#endif
      result = DataTransfer::sendMsg(connection, dataserver.reconfig_query(*key_p, algorithm_name, *conf_id_p));
      break;
    case packet::Operation_MethodType_reconfig_finalize:
      result = DataTransfer::sendMsg(connection,
                                     dataserver.reconfig_finalize(*key_p, *timestamp_p, algorithm_name, *conf_id_p));
      break;
    case packet::Operation_MethodType_reconfig_write:
      result = DataTransfer::sendMsg(connection,
                                     dataserver.reconfig_write(*key_p,
                                                               *value_p,
                                                               *timestamp_p,
                                                               algorithm_name,
                                                               *conf_id_p));
      break;
    case packet::Operation_MethodType_reconfig_finish:
      result = DataTransfer::sendMsg(connection,
                                     dataserver.finish_reconfig(*key_p,
                                                                *timestamp_p,
                                                                to_string(*new_conf_id_p),
                                                                algorithm_name,
                                                                *conf_id_p));
#ifdef CHANGE_THREAD_PRIORITY
      increase_thread_priority(false);
#endif
      break;
    default:DPRINTF(DEBUG_SERVER, "Method Not Found\n");
      EASY_LOG_M("Method Not Found");
      DataTransfer::sendMsg(connection,
                            DataTransfer::serializeToOperation(packet::Operation_MethodType_failure,
                                                               "Unknown method is called"));
  }

  if (result != 1) {
    DPRINTF(DEBUG_SERVER, "Failure: Server Response failed\n");
    EASY_LOG_M("Failure: Server Response failed");
  }

  EASY_LOG_M("Finished");
  DPRINTF(DEBUG_SERVER, "END\n");
}

void connection_handler(int connection, DataServer &dataserver, int portid, const string client_ip = "NULL") {
  DPRINTF(DEBUG_SERVER, "started with port %d\n", portid);
  if (client_ip != "NULL") {
    DPRINTF(DEBUG_METADATA_SERVER, "Incoming connection from %s\n", client_ip.c_str());
  }

#ifdef USE_TCP_NODELAY
  int yes = 1;
  int result = setsockopt(connection, IPPROTO_TCP, TCP_NODELAY, (char *) &yes, sizeof(int));
  if (result < 0) {
    assert(false);
  }
#endif

  while (true) {
    string recvd;
    int result = DataTransfer::recvMsg(connection, recvd);
    if (result != 1) {
      close(connection);
      DPRINTF(DEBUG_METADATA_SERVER, "one connection closed.\n");
      return;
    }
    if (is_warmup_message(recvd)) {
      DPRINTF(DEBUG_METADATA_SERVER, "warmup message received\n");
      string temp = string(WARM_UP_MNEMONIC) + get_random_string();
      result = DataTransfer::sendMsg(connection, temp);
      if (result != 1) {
        close(connection);
        DPRINTF(DEBUG_METADATA_SERVER, "one connection closed.\n");
      }
      continue;
    }

    // Counting the max concurrent ops
//        unique_lock<mutex> ccm(concurrent_counter_mu);
//        concurrent_counter++;
//        if(max_concurrent_counter < concurrent_counter){
//            max_concurrent_counter = concurrent_counter;
//            DPRINTF(DEBUG_METADATA_SERVER, "max concurrent ops on this server is %d\n", max_concurrent_counter);
//        }
//        ccm.unlock();

    message_handler(connection, dataserver, recvd);

    // Counting the max concurrent ops
//        ccm.lock();
//        concurrent_counter--;
//        ccm.unlock();
  }
}

void run_server(const string &db_name, const string &socket_port, const string *socket_ip = nullptr,
                const string &metadata_server_ip = "127.0.0.1", const string &metadata_server_port = "30000") {
  DPRINTF(DEBUG_SERVER, "started.\n");
  DataServer *ds = new DataServer(db_name, socket_setup(socket_port, socket_ip), metadata_server_ip,
                                  metadata_server_port);
  int portid = stoi(socket_port);
  while (1) {
    struct sockaddr_in client_address;
    socklen_t client_address_size = sizeof(client_address);
    int new_sock = accept(ds->getSocketDesc(), (struct sockaddr *) &client_address, &client_address_size);
    if (new_sock < 0) {
      DPRINTF(DEBUG_CAS_Client, "Error: accept: %d, errno is %d\n", new_sock, errno);
    } else {
      char str_temp[INET_ADDRSTRLEN];
      inet_ntop(AF_INET, &(client_address.sin_addr), str_temp, INET_ADDRSTRLEN);
      string client_ip(str_temp);
      thread
          cThread([&ds, new_sock, portid, client_ip]() { connection_handler(new_sock, *ds, portid, client_ip); });
      cThread.detach();
    }
  }

}

void fork_server(vector<pid_t> &children,
                 const string &db_name,
                 const string &socket_port,
                 const string *socket_ip = nullptr,
                 const string &metadata_server_ip = "127.0.0.1",
                 const string &metadata_server_port = "30000") {
  fflush(stdout);
  pid_t child_pid = fork();
  if (child_pid == 0) {
    setbuf(stdout, NULL);
    close(1);
    int pid = getpid();
    stringstream filename;
    filename << "server_" << pid << "_output.txt";
    FILE *out = fopen(filename.str().c_str(), "w");
    setbuf(out, NULL);
    run_server(db_name, socket_port, socket_ip, metadata_server_ip, metadata_server_port);
    exit(0);
  } else if (child_pid < -1) {
    DPRINTF(true, "Fork error with %d\n", child_pid);
    exit(-1);
  } else {
    children.push_back(child_pid);
  }
}

int main(int argc, char **argv) {
  signal(SIGPIPE, SIG_IGN);
  vector<pid_t> children;

  if (argc == 1) {
    for (uint i = 0; i < ports.size(); i++) {
//        if(ports[i] == "10004" || ports[i] == "10005"){
//            continue;
//        }
      fork_server(children, db_names[i], ports[i]);
    }
  } else if (argc == 3) {
    fork_server(children, string(argv[2]) + ".temp", argv[1]);
  } else if (argc == 6) {
    string ip_str(argv[1]);
    fork_server(children, std::string(argv[3]) + ".temp", argv[2], &ip_str, argv[4], argv[5]);
  } else {
    cout << "Enter the correct number of arguments :  ./Server <port_no> <db_name>" << endl;
    cout << "Or : ./Server <ext_ip> <port_no> <db_name> <metadate_server_ip> <metadata_server_port>" <<
         endl;
    return -1;
  }

  cout << "Server forked the children." << endl;
  int ret_val = 0;
  for (auto &pid: children) {
    if (waitpid(pid, &ret_val, 0) != -1) {
      std::cout << "Child " << pid << " temination status " << WIFEXITED(ret_val) << "  Rate receved is "
                << WEXITSTATUS(ret_val) << " : " << WIFSIGNALED(ret_val) << " : " << WTERMSIG(ret_val) << std::endl;
      assert(WEXITSTATUS(ret_val) == 0);
    } else {
      DPRINTF(true, "waitpid error\n");
    }
  }

  return 0;
}
