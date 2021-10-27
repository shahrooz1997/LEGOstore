#include <json.hpp>
#include "../inc/Util.h"
#include <iostream>
#include <algorithm>
#include <sys/wait.h>
#include <cmath>
#include "../inc/Client_Node.h"
#include "../inc/Util.h"
#include <chrono>
#include <map>
#include <cstring>
#include <string>
#include <cstdlib>
#include <fstream>
#include <thread>
#include <arpa/inet.h>

#ifdef LOCAL_TEST
#define DEBUGGING
#endif

using namespace std;
using std::cout;
using std::endl;
using namespace std::chrono;
using json = nlohmann::json;

static uint32_t datacenter_id;
static uint32_t retry_attempts_number;
static uint32_t metadata_server_timeout;
static uint32_t timeout_per_request;
static uint32_t conf_id;
static uint32_t grp_id;
static uint32_t object_size;
static uint64_t num_objects;
// Arrival rate for this specific client in this specific data_center, Dist. * total_arrival_rate.
static double arrival_rate;
static double read_ratio;
static uint64_t run_session_duration;
static vector<DC *> datacenters;
static std::vector<std::string> keys;

enum Op {
  get = 0,
  put
};

class File_logger {
 public:
  explicit File_logger(uint32_t id) : log_filename(string("logs/logfile_") + to_string(getpid()) + ".txt"),
                                      file(nullptr), client_id(id) {
    file = fopen(log_filename.c_str(), "w");
    assert(file != nullptr);

    number_of_ops_to_ignore = NUMBER_OF_OPS_TO_IGNORE;
  }

  void operator()(Op op, const std::string &key, const std::string &value, uint64_t call_time,
                  uint64_t return_time) {
    assert(file != 0);
    if (number_of_ops_to_ignore > 0) {
      number_of_ops_to_ignore--;
      if (number_of_ops_to_ignore <= 0) {
        DPRINTF(DEBUG_CONTROLLER, "Ignoring ops finished\n");
      }
      return;
    }

#ifdef DO_NOT_WRITE_VALUE_IN_LOGS
    fprintf(file, "%u, %s, %s, %s, %lu, %lu\n", client_id, op == Op::get ? "get" : "put", key.c_str(),
            "0", call_time, return_time);
#else
    fprintf(file, "%u, %s, %s, %s, %lu, %lu\n", client_id, op == Op::get ? "get" : "put", key.c_str(),
            value.c_str(), call_time, return_time);
#endif
    fflush(stdout);
  }

  ~File_logger() {
    fclose(file);
  }

 private:
  std::string log_filename;
  FILE *file;
  uint32_t client_id;
  uint32_t number_of_ops_to_ignore;
};

inline uint32_t get_unique_client_id(uint32_t datacenter_id, uint32_t conf_id, uint32_t grp_id, uint32_t req_idx) {
  uint32_t id = 0;
  if ((req_idx < (1 << 12)) && (grp_id < (1 << 7)) && (datacenter_id < (1 << 6)) && (conf_id < (1 << 7))) {
    id = ((datacenter_id << 26) | (conf_id << 19) | (grp_id << 12) | (req_idx));
  } else {
    throw std::logic_error("The thread ID can be redundant. Idx exceeded its bound");
  }
  return id;
}

inline int64_t next_event(const std::string &dist_process) {
  if (dist_process == "poisson") {
    std::default_random_engine generator(system_clock::now().time_since_epoch().count());
#ifdef DEBUGGING
    std::exponential_distribution<double> distribution(1);
#else
    std::exponential_distribution<double> distribution(.5);
#endif

    return duration_cast<milliseconds>(duration<double>(distribution(generator))).count();
//        return -log(1 - (double)rand() / (RAND_MAX)) * 1000;
  } else {
    throw std::logic_error("Distribution process specified is unknown !! ");
  }
}

int read_keys(const std::string &file) {
  std::ifstream cfg(file);
  json j;
  if (cfg.is_open()) {
    cfg >> j;
    if (j.is_null()) {
      DPRINTF(DEBUG_CONTROLLER, "Failed to read the config file\n");
      return -1;
    }
  } else {
    DPRINTF(DEBUG_CONTROLLER, "Couldn't open the config file: %s\n", file.c_str());
    return -1;
  }

  bool keys_set = false;
  j = j["workload_config"];
  for (auto &element: j) {
    if (keys_set)
      break;
    Group_config grpcon;
    element["timestamp"].get_to(grpcon.timestamp);
    element["id"].get_to(grpcon.id);
    if (grpcon.id != conf_id) {
      continue;
    }

    uint32_t counter = 0; // Todo: get rid of this counter
    for (auto &it: element["grp_workload"]) {
      Group grp;
      grp.id = counter++;

      if (grp.id == grp_id) {
        it["keys"].get_to(grp.keys);
//                DPRINTF(DEBUG_ABD_Client, "2Keys are (%lu):\n", grp.keys.size());
//                for(auto& key: grp.keys){
//                    DPRINTF(DEBUG_ABD_Client, "2A%s \n", key.c_str());
//                }
        keys = grp.keys;
        keys_set = true;
        break;
      }
    }
    if (keys_set)
      break;
  }
  cfg.close();
  assert(keys_set);
  assert(keys.size() > 0);
//    DPRINTF(DEBUG_ABD_Client, "Keys are (%lu):\n", keys.size());
//    for(auto& key: keys){
//        DPRINTF(DEBUG_ABD_Client, "A%s \n", key.c_str());
//    }
  return 0;
}

int warm_up_one_connection(const string &ip, uint32_t port) {

  std::string temp = std::string(WARM_UP_MNEMONIC) + get_random_string();
  Connect c(ip, port);
  if (!c.is_connected()) {
    assert(false);
    return -1;
  }
  DataTransfer::sendMsg(*c, temp);
  string recvd;
  if (DataTransfer::recvMsg(*c, recvd) == 1) {
    if (!is_warmup_message(recvd)) {
      assert(false);
    }
  } else {
    assert(false);
  }

  return S_OK;
}

int warm_up() {
  for (auto it = datacenters.begin(); it != datacenters.end(); it++) {
    warm_up_one_connection((*it)->metadata_server_ip, (*it)->metadata_server_port);
    std::this_thread::sleep_for(milliseconds(get_random_number_uniform(0, 2000)));
  }
  for (auto it = datacenters.begin(); it != datacenters.end(); it++) {
    warm_up_one_connection((*it)->servers[0]->ip, (*it)->servers[0]->port);
    std::this_thread::sleep_for(milliseconds(get_random_number_uniform(0, 2000)));
  }
  return S_OK;
}

inline uint32_t ip_str_to_int(const std::string &ip) {
  struct sockaddr_in serv_addr;
  if (inet_pton(AF_INET, ip.c_str(), &(serv_addr.sin_addr)) <= 0) {
    DPRINTF(true, "\nInvalid address/ Address not supported \n");
    assert(false);
  }
  return serv_addr.sin_addr.s_addr;
}

// This function will create a client and start sending requests.
int run_session(uint32_t req_idx) {
  auto start_point = time_point_cast<milliseconds>(system_clock::now());
  int request_counter = 0;
  uint32_t client_id = get_unique_client_id(datacenter_id, conf_id, grp_id, req_idx);

  // Set up seed for random number generator.
#ifdef DEBUGGING
  get_random_number_uniform(0, 100, client_id);
//    srand(client_id);
#else
  uint32_t datacenter_indx = get_datacenter_index(datacenter_id, datacenters);
  get_random_number_uniform(0, 100,
                            time(NULL) + client_id + ip_str_to_int(datacenters[datacenter_indx]->servers[0]->ip));
//    srand(time(NULL) + client_id + ip_str_to_int(datacenters[datacenter_indx]->servers[0]->ip));
#endif
  Client_Node clt(client_id, datacenter_id, retry_attempts_number, metadata_server_timeout, timeout_per_request,
                  datacenters);
  // Randomly delay the first request of clients.
  auto timePoint2 = time_point_cast<milliseconds>(system_clock::now());
  timePoint2 += milliseconds{get_random_number_uniform(0, 2000)};
  std::this_thread::sleep_until(timePoint2);
  // Operation logger.
  File_logger file_logger(clt.get_id());
#ifdef DO_WARM_UP
  // WARM UP THE SOCKETS
  auto timePoint3 += start_point + seconds(WARM_UP_DELAY);
  warm_up();
  std::this_thread::sleep_until(timePoint3);
#endif
  auto end_point = start_point + seconds(run_session_duration);
  auto op_start_tp = time_point_cast<milliseconds>(system_clock::now());
  while (system_clock::now() < end_point) {
    request_counter += 1;
    Op req_type = get_random_real_number_uniform(0, 1) < read_ratio ? Op::get : Op::put;
    int result = S_OK;
    // Choose a random key.
    DPRINTF(DEBUG_ABD_Client, "Number of keys is %lu\n", keys.size());
    uint key_idx = (uint) get_random_number_uniform(0, keys.size() - 1);
    DPRINTF(DEBUG_ABD_Client, "chosen key index is %u\n", key_idx);
    if (req_type == Op::get) {
      std::string read_value;
      auto epoch = time_point_cast<std::chrono::microseconds>(
          std::chrono::system_clock::now()).time_since_epoch().count();
      result = clt.get(keys[key_idx], read_value);
      if (result != 0) {
        DPRINTF(DEBUG_CAS_Client, "clt.get on key %s, result is %d\n", keys[key_idx].c_str(), result);
        assert(false);
      }
      auto epoch2 = time_point_cast<std::chrono::microseconds>(
          std::chrono::system_clock::now()).time_since_epoch().count();
      file_logger(Op::get, keys[key_idx], read_value, epoch, epoch2);
      DPRINTF(DEBUG_CAS_Client, "get done on key: %s with value: %s\n", keys[key_idx].c_str(),
              (TRUNC_STR(read_value)).c_str());
    } else { // Op::put
      std::string val = get_random_string(object_size);
      auto epoch = time_point_cast<std::chrono::microseconds>(
          std::chrono::system_clock::now()).time_since_epoch().count();
      result = clt.put(keys[key_idx], val);
      if (result != 0) {
        DPRINTF(DEBUG_CAS_Client, "clt.put on key %s, result is %d\n", keys[key_idx].c_str(), result);
        assert(false);
      }
      auto epoch2 = time_point_cast<std::chrono::microseconds>(
          std::chrono::system_clock::now()).time_since_epoch().count();
      file_logger(Op::put, keys[key_idx], val, epoch, epoch2);
      DPRINTF(DEBUG_CAS_Client, "put done on key: %s with value: %s\n", keys[key_idx].c_str(),
              std::string(TRUNC_STR(val)).c_str());
    }
    op_start_tp += milliseconds{next_event("poisson")};
    std::this_thread::sleep_until(op_start_tp);
  }

  // Sleep for 10 seconds for threads running asynchronous phase.
#ifdef LOCAL_TEST
  std::this_thread::sleep_for(seconds(1));
#else
  std::this_thread::sleep_for(seconds(10));
#endif
  return request_counter;
}

//NOTE:: A process is started for each grp_id
//NOTE:: Grp_id is just a unique identifier for this grp, can be modified easily later on
int create_clients() {
  int avg = 0;
  long int numReqs = lround(arrival_rate);
  assert(numReqs >= 0);
  vector<pid_t> children;
#ifdef DEBUGGING
  for(long int i = 0; i < numReqs; i++){
#else
  for (long int i = 0; i < 2 * numReqs; i++) {
#endif
    fflush(stdout);
    pid_t child_pid = fork();
    if (child_pid == 0) {
      std::setbuf(stdout, NULL);
      close(1);
      int pid = getpid();
      std::stringstream filename;
      filename << "client_" << pid << "_output.txt";
      FILE *out = fopen(filename.str().c_str(), "w");
      std::setbuf(out, NULL);
      int cnt = run_session(static_cast<uint32_t>(i));
      exit(cnt);
    } else if (child_pid < -1) {
      DPRINTF(true, "Fork error with %d\n", child_pid);
      exit(-1);
    } else {
      children.push_back(child_pid);
    }
  }
  int total = 0;
  int ret_val = 0;
  for (auto &pid: children) {
    if (waitpid(pid, &ret_val, 0) != -1) {
      std::cout << "Child " << pid << " temination status " << WIFEXITED(ret_val) << "  Rate receved is "
                << WEXITSTATUS(ret_val) << " : " << WIFSIGNALED(ret_val) << " : " << WTERMSIG(ret_val) << std::endl;
      total += WEXITSTATUS(ret_val);
    } else {
      DPRINTF(true, "waitpid error\n");
    }
  }
  avg = total / run_session_duration;
  std::cout << "Average arrival Rate : " << avg << std::endl;
  if (avg < numReqs) {
    fprintf(stdout,
            "WARNING!! The average arrival rate for group :%d and group_config :%d is less than required : %u / %ld\n",
            conf_id,
            grp_id,
            avg,
            numReqs);
  }
  return avg;
}

int read_detacenters_info(const std::string &file) {
  std::ifstream cfg(file);
  json j;
  if (cfg.is_open()) {
    cfg >> j;
    if (j.is_null()) {
      DPRINTF(DEBUG_CONTROLLER, "Failed to read the config file\n");
      return -1;
    }
  } else {
    DPRINTF(DEBUG_CONTROLLER, "Couldn't open the config file: %s\n", file.c_str());
    return -1;
  }
  for (auto &it: j.items()) {
    DC *dc = new DC;
    dc->id = stoui(it.key());
    it.value()["metadata_server"]["host"].get_to(dc->metadata_server_ip);
    dc->metadata_server_port = stoui(it.value()["metadata_server"]["port"].get<std::string>());
    for (auto &server: it.value()["servers"].items()) {
      Server *sv = new Server;
      sv->id = stoui(server.key());
      server.value()["host"].get_to(sv->ip);
      sv->port = stoui(server.value()["port"].get<std::string>());
      sv->datacenter = dc;
      dc->servers.push_back(sv);
    }
    datacenters.push_back(dc);
  }
  cfg.close();
  return 0;
}

int main(int argc, char *argv[]) {
  if (argc != 12) {
    std::cout << "Usage: " << argv[0] << " " << "<datacenter_id> <retry_attempts_number> "
                                                "<metadata_server_timeout> <timeout_per_request> <conf_id> "
                                                "<grp_id> <object_size> <arrival_rate> <read_ratio> "
                                                "<duration> <num_objects>" << std::endl;
    return 1;
  }
  datacenter_id = stoul(argv[1]);
  retry_attempts_number = stoul(argv[2]);
  metadata_server_timeout = stoul(argv[3]);
  timeout_per_request = stoul(argv[4]);
  conf_id = stoul(argv[5]);
  grp_id = stoul(argv[6]);
  object_size = stoul(argv[7]);
  arrival_rate = stod(argv[8]);
  read_ratio = stod(argv[9]);
  run_session_duration = stoull(argv[10]);
  num_objects = stoul(argv[11]);

  // It reads datacenter info from the config file.
#ifdef LOCAL_TEST
  assert(read_detacenters_info("./config/local_config.json") == 0);
#else
  assert(read_detacenters_info("./config/auto_test/datacenters_access_info.json") == 0);
#endif
  assert(read_keys("./config/auto_test/input_workload.json") == 0);

  // Create clients to send requests.
  create_clients();
  return 0;
}
