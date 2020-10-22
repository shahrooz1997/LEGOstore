#include <iostream>
#include <string>
#include <vector>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sstream>
#include <cstring>
#include <fstream>
#include <thread>
#include <map>
#include <algorithm>
#include <ctime>
#include <sstream>
#include <cstdlib>
#include <cstdio>

// #define METADATA_IP     machines[0].get_external_ip()
#define METADATA_PORT   "30000"

#define MACHINE_TYPE    "e2-standard-2" //"e2-micro"

#define INIT


using namespace std;

enum Status{
    OK = 0,
    machine_init,
    machine_running,
    pipe_error,
    fork_error,
    child_read_error,
    chile_exec_error,
    created_machine_error,
    status_is_not_running,
    scp_exec_error,
    unknown_error,
    resource_unavailable
};

namespace Helpers{
	int execute(const char* command, const vector<string>& args, string& output){

	    cout << "Executing \"" << command << " ";
	    for(int i = 1; i < args.size(); i++){
	        cout << args[i];
	        if(i != args.size() - 1)
	            cout << " ";
	    }
	    cout << "\"...\n";


        int pipedes[2];
        if(pipe(pipedes) == -1) {
            perror("pipe");
            return Status::pipe_error;
        }

        // int pipedes2[2];
        // if(pipe(pipedes2) == -1) {
        //     perror("pipe");
        //     return Status::pipe_error;
        // }


        pid_t pid = fork();
        if (pid == -1){
            perror("fork");
            return Status::fork_error;
        }
        else if(pid == 0){ // Child process
            while ((dup2(pipedes[1], STDOUT_FILENO) == -1) && (errno == EINTR)) {} // we used while so it will happen even if an interrupt arises
            while ((dup2(pipedes[1], STDERR_FILENO) == -1) && (errno == EINTR)) {}
            close(pipedes[1]);
            close(pipedes[0]);
            // while ((dup2(pipedes2[2], STDIN_FILENO) == -1) && (errno == EINTR)) {}
            // close(pipedes2[1]);
            // close(pipedes2[0]);
            switch(args.size()){
                case 1:
                    execl(command, args[0].c_str(), (char *) 0);
                    break;

                case 2:
                    execl(command, args[0].c_str(), args[1].c_str(), (char *) 0);
                    break;

                case 3:
                    execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), (char *) 0);
                    break;

                case 4:
                    execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), args[3].c_str(), (char *) 0);
                    break;

                case 5:
                    execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), args[3].c_str(), args[4].c_str(), (char *) 0);
                    break;

                case 6:
                    execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), args[3].c_str(), args[4].c_str(), args[5].c_str(), (char *) 0);
                    break;

                case 7:
                    execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), args[3].c_str(), args[4].c_str(), args[5].c_str(), args[6].c_str(), (char *) 0);
                    break;

                case 8:
                    execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), args[3].c_str(), args[4].c_str(), args[5].c_str(), args[6].c_str(), args[7].c_str(), (char *) 0);
                    break;

                case 9:
                    execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), args[3].c_str(), args[4].c_str(), args[5].c_str(), args[6].c_str(), args[7].c_str(), args[8].c_str(), (char *) 0);
                    break;

                case 10:
                    execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), args[3].c_str(), args[4].c_str(), args[5].c_str(), args[6].c_str(), args[7].c_str(), args[8].c_str(), args[9].c_str(), (char *) 0);
                    break;

                case 11:
                    execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), args[3].c_str(), args[4].c_str(), args[5].c_str(), args[6].c_str(), args[7].c_str(), args[8].c_str(), args[9].c_str(), args[10].c_str(), (char *) 0);
                    break;
            }
//            for(int i = 0; i < args.size(); i++){
//                delete args_c[i];
//            }
//            delete args_c;
            perror("execl");
            _exit(1);
        }
        else {
//            for(int i = 0; i < args.size(); i++){
//                delete args_c[i];
//            }
//            delete args_c;
            close(pipedes[1]);
            // close(pipedes2[2]);
            int ret_val;
            wait(&ret_val);

            ret_val = WEXITSTATUS(ret_val);

            char buffer[4096];
            while(true){
                ssize_t count = read(pipedes[0], buffer, sizeof(buffer));
                if (count == -1) {
                    if (errno == EINTR) {
                        continue;
                    }
                    else{
                        perror("read");
                        return Status::child_read_error;
                    }
                }
                else if(count == 0){
                    break;
                }
                else{
                    output.append(buffer, count);
                }
            }
            close(pipedes[0]);
            return ret_val;
        }
	}

    std::string execCommand(const std::string& cmd, int& out_exitStatus){
        out_exitStatus = 0;
        auto pPipe = ::popen(cmd.c_str(), "r");
        if(pPipe == nullptr)
        {
            throw std::runtime_error("Cannot open pipe");
        }

        std::array<char, 256> buffer;

        std::string result;

        while(not std::feof(pPipe))
        {
            auto bytes = std::fread(buffer.data(), 1, buffer.size(), pPipe);
            result.append(buffer.data(), bytes);
        }

        auto rc = ::pclose(pPipe);

        if(WIFEXITED(rc))
        {
            out_exitStatus = WEXITSTATUS(rc);
        }

        return result;
    }

    void scp(const std::string& host_ip, const std::string& src_path, const std::string& dst_path){

        // srand(time(nullptr));
        // std::string output_file_name = "/tmp/myscp_";
        // output_file_name += to_string(rand() % 10000);
        // output_file_name += ".txt";

        std::string command = "scp -qo StrictHostKeyChecking=no -r ";
        // command += host_ip + " ";
        command += "'";
        command += src_path;
        command += "' ";
        command += host_ip;
        command += ":";
        command += dst_path;
        // command += "> ";
        // command += output_file_name;
        // command += " 2>&1";

        cout << "command to be executed is " << command << endl;
        // system(command.c_str());
        int t;
        execCommand(command, t);

        // ifstream in(output_file_name, ios::in);
        
        // ostringstream ss;
        // ss << in.rdbuf(); // reading data
        // std::string output = ss.str();
        // in.close();

        // command.clear();
        // command = "rm -rf ";
        // command += output_file_name;
        // system(command.c_str());

        cout << "scp done" << endl;

        return;
    }

    void scp_from(const std::string& host_ip, const std::string& src_path, const std::string& dst_path){

        // srand(time(nullptr));
        // std::string output_file_name = "/tmp/myscp_";
        // output_file_name += to_string(rand() % 10000);
        // output_file_name += ".txt";

        std::string command = "scp -qo StrictHostKeyChecking=no -r ";
        // command += host_ip + " ";
        command += host_ip;
        command += ":";
        command += src_path;
        command += " '";
        command += dst_path;
        command += "'";
        // command += "> ";
        // command += output_file_name;
        // command += " 2>&1";

        cout << "command to be executed is " << command << endl;
        // system(command.c_str());
        int t;
        execCommand(command, t);

        // ifstream in(output_file_name, ios::in);
        
        // ostringstream ss;
        // ss << in.rdbuf(); // reading data
        // std::string output = ss.str();
        // in.close();

        // command.clear();
        // command = "rm -rf ";
        // command += output_file_name;
        // system(command.c_str());

        cout << "scp done" << endl;

        return;
    }

    std::string ssh(const std::string& host_ip, const std::string& cmd){

        
        // std::string output_file_name = "/tmp/myssh_";
        // output_file_name += to_string(rand() % 10000);
        // output_file_name += ".txt";

        std::string command = "ssh -qo StrictHostKeyChecking=no -t ";
        command += host_ip + " ";
        command += "'";
        command += cmd;
        command += "' ";
        // command += "> ";
        // command += output_file_name;
        // command += " 2>&1";

        cout << "command to be executed is " << command << endl;
        // system(command.c_str());
        int t;
        std::string output = execCommand(command, t);

        // ifstream in(output_file_name, ios::in);
        
        // ostringstream ss;
        // ss << in.rdbuf(); // reading data
        // std::string output = ss.str();
        // in.close();

        // command.clear();
        // command = "rm -rf ";
        // command += output_file_name;
        // system(command.c_str());

        cout << "output for command " << cmd << " is " << output << endl;

        return output;
    }
}

class Machine{
private:
	string name;
	string type;
	string zone;
	int number_of_cpu;  // For later use
	int ram_size;	    // For later use

	string internal_ip;
	string external_ip;

	int status;



public:
    bool existed;

    Machine(){
        number_of_cpu = 0;
        ram_size = 0;

        status = machine_init;
    }

    Machine(const string& name, const string& type, const string& zone, const string& internal_ip,
        const string& external_ip) : name(name), type(type), zone(zone), internal_ip(internal_ip),
        external_ip(external_ip){
        
        status = Status::machine_running;
    }

	Machine(const string& name, const string& type, const string& zone) : name(name), type(type), zone(zone){
        existed = false;
		
		//ToDo: obtain this information from the google cloud
		number_of_cpu = 0;
		ram_size = 0;

		status = Status::machine_init;


		// run the server
        vector<string> args = {"gcloud", "compute", "instances", "create", this->name, string("--machine-type=") + this->type, string("--zone=") + this->zone};
        string output;
        status = Helpers::execute("/home/shahrooz/google-cloud-sdk/bin/gcloud", args, output);

//        cout << status << "AAA " << endl;
		
        if(output != ""){
            if(output.find("ERROR") != string::npos){ // ToDo: handle other errors specially limit-reached error

                if(output.find("enough resources available") != string::npos){
                    status = Status::resource_unavailable;
                }
                else if(output.find("already exists") != string::npos){
                    existed = true;
                    status = Status::chile_exec_error;
                    cerr << output << endl;
                    vector<string> args = {"gcloud", "compute", "instances", "list"};
                    output.clear();
                    status = Helpers::execute("/home/shahrooz/google-cloud-sdk/bin/gcloud", args, output);
    //                cout << "SSS: " << output << endl;
                    string line;
                    stringstream s(output);
                    while(!s.eof()){
                        getline(s, line);
                        size_t pos;
                        pos = line.find(' ');
                        if(line.substr(0, pos) == this->name){
                            stringstream s(line);
    //                cout << "AAA: " << output.substr(pos + 1) << endl;
                            string a1, a2, a3, a4;
                            s >> skipws >> a1 >> a2 >> a3 >> this->internal_ip >> this->external_ip >> a4;
                            if(a4 == "RUNNING"){
    //                            cerr << "yeah: " << output << endl;
                                status = Status::machine_running;
                            }
                            else{
                                cerr << output << endl;
                                status = Status::created_machine_error;
                            }
                        }
                    }
                }
                else{
                    cout << "UNKNOWN ERROR: " << output << endl;
                    status = Status::unknown_error;
                }
            }
            else{
                size_t pos;
                pos = output.find('\n');
                pos = output.find('\n', pos + 1);
//                cout << pos << " SSS" << endl;
                stringstream s(output.substr(pos + 1));
//                cout << "AAA: " << output.substr(pos + 1) << endl;
                string a1, a2, a3, a4;
                s >> skipws >> a1 >> a2 >> a3 >> this->internal_ip >> this->external_ip >> a4;
                if(a4 == "RUNNING"){
                    cerr << output << endl;
                    status = Status::machine_running;
                }
                else{
                    cerr << output << endl;
                    status = Status::created_machine_error;
                }
            }
        }
        else{
            status = Status::chile_exec_error;
            cerr << output << endl;
        }

	}


	~Machine(){}

    int disk_resize(int size_gb){
        if(this->status != Status::machine_running) {
            return Status::status_is_not_running;
        }

        vector<string> args = {"gcloud", "compute", "disks", "resize", this->name, string("--size=") + to_string(size_gb), string("--zone=") + this->zone, "-q"};
        string output;
        status = Helpers::execute("/home/shahrooz/google-cloud-sdk/bin/gcloud", args, output);

//        cout << status << "AAA " << endl;
        
        if(output != ""){
            if(output.find("ERROR") != string::npos){ // ToDo: handle other errors specially limit-reached error

                cout << "UNKNOWN ERROR in resize: " << output << endl;
                status = Status::unknown_error;
            }
        }
        else{
            status = Status::chile_exec_error;
            cerr << output << endl;
        }
    }

	string get_name() const{
	    return name;
	}
	string get_external_ip() const {
	    return external_ip;
	}
    string get_internal_ip() const {
        return internal_ip;
    }
	int get_status() const {
        return status;
	}

	int copy_to(const string& path){

	    if(this->status != Status::machine_running) {
            return Status::status_is_not_running;
        }

        // vector<string> args = {"scp", "-o", "StrictHostKeyChecking=no", "-r", path, this->external_ip + ":"};
        // string output;
        // int status = Helpers::execute("/usr/bin/scp", args, output);
        // cerr << output << endl;

        // if(output != ""){
        //     if(output.find("ERROR") != string::npos){
        //         return scp_exec_error;
        //     }
        //     else{
        //         return Status::OK;
        //     }
        // }
        // else{
        //     cerr << "status is " << status << endl;
        //     return Status::scp_exec_error;
        // }

        Helpers::scp(this->external_ip, path, "");



	}

    int copy_from(const string& src, const string& dst){

        if(this->status != Status::machine_running) {
            return Status::status_is_not_running;
        }

        Helpers::scp_from(this->external_ip, src, dst);

    }

	int execute(const string& command){

        if(this->status != Status::machine_running) {
            return Status::status_is_not_running;
        }

        // vector<string> args = {"ssh", "-o", "StrictHostKeyChecking=no", "-t", this->external_ip, command};
        // string output;
        // int status = Helpers::execute("/usr/bin/ssh", args, output);

        // if(status == Status::OK){
        //     // cout << output << endl;
        //     return Status::OK;
        // }
        // else{
        //     if(output != ""){
        //         if(output.find("ERROR") != string::npos){
        //             cerr << "error in execution " << command << endl;
        //             // cerr << output << endl;
        //             return scp_exec_error;
        //         }
        //            else{
        //               return Status::OK;
        //            }
        //     }
        //     else{
        //         cerr << "WARN: ret value in execution " << command << " is not zero and output is empty." << endl;
        //         return scp_exec_error;
        //     }
        // }

        std::string output = Helpers::ssh(this->external_ip, command);
        if(output != ""){
            if(output.find("ERROR") != string::npos){
                cerr << "error in execution " << command << endl;
                // cerr << output << endl;
                return scp_exec_error;
            }
            else{
                return Status::OK;
            }
        }
        else{
            // cerr << "WARN: ret value in execution " << command << " output is empty." << endl;
            return scp_exec_error;
        }


	}

    std::string execute2(const string& command){

        if(this->status != Status::machine_running) {
            return "";
        }

        // vector<string> args = {"ssh", "-o", "StrictHostKeyChecking no", "-t", this->external_ip, command};
        // string output;
        // int status = Helpers::execute("/usr/bin/ssh", args, output);

        // return output;

        return Helpers::ssh(this->external_ip, command);

    }
};

static vector<Machine> machines;
static string setup_config;
static vector<string> regions;// = {"us-central1-a", "us-east4-a", "australia-southeast1-c", "southamerica-east1-a", "asia-southeast2-a",
                        // "northamerica-northeast1-a", "europe-north1-a", "asia-east2-c", "europe-west6-b", "asia-northeast2-b",
                        // "asia-south1-b", "us-west3-a"};
static std::vector<string> zones_regions;
static std::map<std::string, vector<std::string> > zones_name;

template <class Container>
void splitbyline(const std::string& str, Container& cont,
              char delim = '\n')
{
    std::size_t current, previous = 0;
    current = str.find(delim);
    while (current != std::string::npos) {
        cont.push_back(str.substr(previous, current - previous));
        previous = current + 1;
        current = str.find(delim, previous);
    }
    cont.push_back(str.substr(previous, current - previous));
}

void split(const std::string& str, std::vector<std::string>& cont){
    std::size_t current, previous = 0;
    current = str.find(" ");
    if(current == 0){
        previous = str.find_first_not_of(" ", current);
        current = str.find(" ", previous);
    }
    while (current != std::string::npos) {
        cont.push_back(str.substr(previous, current - previous));
        // previous = current + 1;
        previous = str.find_first_not_of(" ", current + 1);
        current = str.find(" ", previous);
    }
    cont.push_back(str.substr(previous, current - previous));

    // for(int i = 0; i < cont.size(); i++){
    //     std::cout << "| "<< cont[i] <<" |" << std::endl;
    // }

}

int fill_zones_name(){

    vector<string> args = {"gcloud", "compute", "zones", "list"};
    string output;
    int status = Helpers::execute("/home/shahrooz/google-cloud-sdk/bin/gcloud", args, output);

    std::vector<std::string> lines;
    splitbyline(output, lines);

    for(uint i = 1; i < lines.size(); i++){
        std::string& line = lines[i];
        if(line.find("UP") != std::string::npos){
            std::vector<std::string> v;
            split(line, v);
            if(std::find(regions.begin(), regions.end(), v[1]) == regions.end()){
                regions.push_back(v[1]);
            }
            zones_name[v[1]].push_back(v[0]);
        }
    }

    return 0;
}

int write_setup_config(){
    setup_config = "{\n";
    for(int i = 0; i < machines.size(); i++){
        setup_config += "    \"";
        setup_config += to_string(i + 1);
        setup_config += "\" : {\n";

        setup_config += "        \"metadata_server\" : {\n";
        setup_config += "            \"host\" : \"";
        setup_config += machines[i].get_external_ip();
        setup_config += "\",\n";
        setup_config += "            \"port\" : \"";
        setup_config += METADATA_PORT;
        setup_config += "\"\n";
        setup_config += "        },\n";
        setup_config += "        \"servers\" : {\n";
        setup_config += "            \"1\" : {\n";
        setup_config += "                \"host\" : \"";
        setup_config += machines[i].get_external_ip();
        setup_config += "\",\n";
        setup_config += "                \"port\" : \"10000\"\n";
        setup_config += "            }\n";
        setup_config += "        }\n";

        setup_config += "    }";
        if(i != machines.size() - 1){
            setup_config += ",\n";
        }
        else{
            setup_config += "\n";
        }
    }

    setup_config += "}";

    ofstream out("setup_config.json", ios::out);
    out << setup_config;
    out.close();

    return 0;
}

const Machine* get_machine_by_name(const string& name, const vector<Machine>& vec){
    for(int i = 0; i < vec.size(); i++){
        if(vec[i].get_name() == name){
            return &(vec[i]);
        }
    }
    return nullptr;
}

int create_machines(uint number_of_machines){

    vector<Machine> existing_machines;
    vector<Machine> created_machines;

    // Todo: you do the code below after fetching existing machines
    // ifstream in("REGIONS.txt", ios::in);
    // std::vector<string> default_zones;
    // while(!in.eof()){
    //     string temp;
    //     in >> temp;
    //     default_zones.push_back(temp);
    // }
    // in.close();

    // for(int i = 0; i < default_zones.size(); i++){
    //     if(default_zones[i] == ""){
    //         continue;
    //     }
    //     string name = "s";
    //     name += to_string(i + 1);
    //     machines.push_back(Machine(name, MACHINE_TYPE, default_zones[i]));
    // }
    // for(int i = 0; i < machines.size(); i++){
    //     machines[i].disk_resize(20);
    // }

    // return 0;




    // fetching existing machines
    std::vector<std::string> server_names;
    std::map<std::string, bool> server_name_is_there;

    for(int i = 0; i < number_of_machines; i++){
        server_names.push_back(string("s") + to_string(i + 1));
        server_name_is_there[string("s") + to_string(i + 1)] = false;
    }
    vector<string> args = {"gcloud", "compute", "instances", "list"};
    std::string output;
    Helpers::execute("/home/shahrooz/google-cloud-sdk/bin/gcloud", args, output);
//                cout << "SSS: " << output << endl;
    string line;
    stringstream s(output);
    while(!s.eof()){
        getline(s, line);
        size_t pos;
        pos = line.find(' ');
        if(std::find(server_names.begin(), server_names.end(), line.substr(0, pos)) != server_names.end()){
            stringstream s(line);
//                cout << "AAA: " << output.substr(pos + 1) << endl;
            string a1, a2, a3, a4, a5, a6;
            s >> skipws >> a1 >> a2 >> a3 >> a4 >> a5 >> a6;
            if(a6 == "RUNNING"){
//                            cerr << "yeah: " << output << endl;
                Machine m1(a1, a3, a2, a4, a5);
                existing_machines.push_back(m1);
                server_name_is_there[a1] = true;
                regions.erase(std::find(regions.begin(), regions.end(), a2.substr(0, a2.size() - 2)));
                // if(existing_machines.size() == number_of_machines)
                //     break;
            }
            else if(a6 != "EXTERNAL_IP"){
                cerr << line << endl;
            }
        }
        else{
            stringstream s(line);
//                cout << "AAA: " << output.substr(pos + 1) << endl;
            string a1, a2, a3, a4, a5, a6;
            s >> skipws >> a1 >> a2 >> a3 >> a4 >> a5 >> a6;
            if(a6 == "RUNNING"){
//                            cerr << "yeah: " << output << endl;
                regions.erase(std::find(regions.begin(), regions.end(), a2.substr(0, a2.size() - 2)));
                // if(existing_machines.size() == number_of_machines)
                //     break;
            }
            else if(a6 != "EXTERNAL_IP"){
                cerr << line << endl;
            }
        }
    }

    int n_creating_machines = number_of_machines - existing_machines.size();

    for(uint i = 0; i < regions.size() && created_machines.size() < n_creating_machines; i++){
        Machine m1;
        uint j = 0;
        std::string name;
        do{
            for(int j = 0; j < server_names.size(); j++){
                if(server_name_is_there[server_names[j]] == false){
                    name = server_names[j];
                    break;
                }
            }
            m1 = Machine(name, MACHINE_TYPE, zones_name[regions[i]][j]);
            j++;
        } while(m1.get_status() == Status::resource_unavailable && j < zones_name[regions[i]].size());

        if(m1.get_status() == Status::machine_running){
            created_machines.push_back(m1);
            server_name_is_there[name] = true;
        }
    }

    // Combine create machines with existing machines
    bool is_a_machine_created = false;
    for(int i = 0; i < server_names.size(); i++){
        const Machine* ptr1 = get_machine_by_name(server_names[i], existing_machines);
        if(ptr1 != nullptr){
            machines.push_back(*ptr1);
        }
        else if((ptr1 = get_machine_by_name(server_names[i], created_machines)) != nullptr){
            machines.push_back(*ptr1);
            is_a_machine_created = true;
        }
        else{
            return -1;
        }
    }

    if(machines.size() < number_of_machines)
        return -1;

    for(int i = 0; i < machines.size(); i++){
        cout << machines[i].get_name() << ": " << machines[i].get_external_ip() << endl;
    }

    if(is_a_machine_created){
        sleep(20);
        for(int i = 0; i < machines.size(); i++){
            machines[i].disk_resize(20);
        }
    }


    return 0;
}

void _config_machines(int i, bool make_clear){

    // cout << "thread for machine " << i << "started" << endl;

    if(machines[i].execute2("ls project").find("lib") != std::string::npos){
        machines[i].copy_to("/home/shahrooz/Desktop/PSU/Research/LEGOstore/scripts/project2.tar.gz");
        machines[i].execute("tar -xzf project2.tar.gz");
    }
    else{
        // Project compile requirement
        machines[i].execute("sudo apt-get install -y build-essential autoconf automake libtool zlib1g-dev git protobuf-compiler pkg-config psmisc bc > /dev/null 2>&1");
        machines[i].execute("git clone https://github.com/openstack/liberasurecode.git > /dev/null 2>&1");
        machines[i].execute("cd liberasurecode/; ./autogen.sh > /dev/null 2>&1; ./configure --prefix=/usr > /dev/null 2>&1; make > /dev/null 2>&1");
        machines[i].execute("cd liberasurecode/; sudo make install > /dev/null 2>&1");
        machines[i].copy_to("/home/shahrooz/Desktop/PSU/Research/LEGOstore/scripts/project.tar.gz");
        machines[i].execute("tar -xzf project.tar.gz");
    }
    // machines[i].execute("cd project/; make proto > /dev/null 2>&1; make cleanall > /dev/null 2>&1; make > /dev/null 2>&1"); // compile the project
    if(make_clear){
        machines[i].execute("cd project/; make proto > /dev/null 2>&1; make cleanall; make > /dev/null 2>&1"); // compile the project
    }
    else{
        machines[i].execute("cd project/; make proto > /dev/null 2>&1; make > /dev/null 2>&1"); // compile the project
    }
    return;
}

int config_machines(bool make_clear){
    
    system("./copy2.sh"); // partial copy
    system("./copy.sh"); // Full copy
    
    vector <thread*> thread_pool;

    for(int i = 0; i < machines.size(); i++) {
        thread* ptr = new thread(_config_machines, i, make_clear);
        thread_pool.push_back(ptr);
    }

    for(int i = 0; i < thread_pool.size(); i++){
        thread_pool[i]->join();
        delete thread_pool[i];
    }

    return 0;
}

int execute_server(const vector<int>& server_numbers){
    vector <thread> thread_pool;

    for(int i = 0; i < server_numbers.size(); i++){
        int index = server_numbers[i];
        thread_pool.emplace_back([index]() {
            std::string command;
            command = "cd project/; make cleandb > /dev/null 2>&1; ./Server ";
            command += machines[index].get_internal_ip();
            command += " 10000 db ";
            command += machines[index].get_internal_ip();
            command += " ";
            command += METADATA_PORT;
            command += " > server_output.txt 2>&1";
            machines[index].execute(command);
        });
    }

    for(int i = 0; i < thread_pool.size(); i++){
        thread_pool[i].join();
    }

    return 0;
}

int manual_execute_command(const vector<int>& server_numbers, const string& command){
    vector <thread> thread_pool;

    for(int i = 0; i < server_numbers.size(); i++){
        int index = server_numbers[i];
        thread_pool.emplace_back([index, command]() {
            machines[index].execute(command);
        });
    }

    for(int i = 0; i < thread_pool.size(); i++){
        thread_pool[i].join();
    }

    return 0;
}

int stop_server(const vector<int>& server_numbers){
    vector <thread> thread_pool;

    for(int i = 0; i < server_numbers.size(); i++){
        int index = server_numbers[i];
        thread_pool.emplace_back([index]() {
            std::string command;
            command = "killall Server Metadata_Server";
            machines[index].execute(command);
        });
    }

    for(int i = 0; i < thread_pool.size(); i++){
        thread_pool[i].join();
    }

    return 0;
}

int execute_metadata_server(const vector<int>& server_numbers){

    vector <thread> thread_pool;

    for(int i = 0; i < server_numbers.size(); i++){
        int index = server_numbers[i];
        thread_pool.emplace_back([index]() {
            std::string command;
            command = "cd project/; ./Metadata_Server ";
            command += machines[index].get_internal_ip() + " ";
            command += METADATA_PORT;
            command += " > metadata_output.txt 2>&1";
            machines[index].execute(command);
        });
    }

    for(int i = 0; i < thread_pool.size(); i++){
        thread_pool[i].join();
    }

    return 0;
}

void _get_pairwise_latency(int i){

    machines[i].copy_to("./myping");
    string command = "./myping ";
    command += to_string(i + 1) + " ";
    for(int j = 0; j < machines.size(); j++){
        command += machines[j].get_external_ip() + " ";
    }

    machines[i].execute(command);

    machines[i].copy_from(string("latencies_from_server_") + to_string(i + 1) + ".txt", "pairwise_latencies");

    return;
}

int get_pairwise_latency(){
    system("cd ./pairwise_latencies; rm -f latencies*");
    vector <thread*> thread_pool;

    for(int i = 0; i < machines.size(); i++) {
        thread* ptr = new thread(_get_pairwise_latency, i);
        thread_pool.push_back(ptr);
    }

    for(int i = 0; i < thread_pool.size(); i++){
        thread_pool[i]->join();
        delete thread_pool[i];
    }

    system("cd ./pairwise_latencies; ./combine.sh");

    return 0;
}

void _clean_up(int i){

    machines[i].execute("rm -rf *");
    return;
}

int clean_up(){
    vector <thread*> thread_pool;

    for(int i = 0; i < machines.size(); i++) {
        thread* ptr = new thread(_clean_up, i);
        thread_pool.push_back(ptr);
    }

    for(int i = 0; i < thread_pool.size(); i++){
        thread_pool[i]->join();
        delete thread_pool[i];
    }
}

void _retrieve_logs(const string& run_type, int i){

    machines[i].copy_from(string("project/logs/*"), string("data/") + run_type + "/s" + to_string(i + 1) + "/logs");

    return;
}

int retrieve_logs(const string& run_type){

    // system((string("rm -f data/") + ).c_str());

    vector <thread*> thread_pool;

    for(int i = 0; i < machines.size(); i++) {
        system((string("mkdir -p data/") + run_type + "/s" + to_string(i + 1) + "/logs").c_str());
        thread* ptr = new thread(_retrieve_logs, run_type, i);
        thread_pool.push_back(ptr);
    }

    for(int i = 0; i < thread_pool.size(); i++){
        thread_pool[i]->join();
        delete thread_pool[i];
    }

    return 0;
}

void _retrieve_data(const string& run_type, int i){

    machines[i].copy_from(string("project/*_output.txt"), string("data/") + run_type + "/s" + to_string(i + 1));

    return;
}

int retrieve_data(const string& run_type){

    system((string("rm -rf data/") + run_type + "/*").c_str());
    
    // vector <thread*> thread_pool;

    // for(int i = 0; i < machines.size(); i++) {
    //     system((string("mkdir -p data/") + run_type + "/s" + to_string(i + 1)).c_str());

    //     thread* ptr = new thread(_retrieve_data, run_type, i);
    //     thread_pool.push_back(ptr);
    // }

    // for(int i = 0; i < thread_pool.size(); i++){
    //     thread_pool[i]->join();
    //     delete thread_pool[i];
    // }

    retrieve_logs(run_type);

    return 0;

}

int retrieve_sum(const vector<int>& server_numbers, const string& run_type){

    vector <thread> thread_pool;

    for(int i = 0; i < server_numbers.size(); i++){
        int index = server_numbers[i];
        thread_pool.emplace_back([index, run_type]() {
            std::string command;
            command = "cd project; ./summarize.sh ";
            command += run_type;
            command += " > sum.txt 2>&1";
            machines[index].execute(command);
        });
    }

    for(int i = 0; i < thread_pool.size(); i++){
        thread_pool[i].join();
    }

    thread_pool.clear();

    system((string("rm -rf data/") + run_type + "/*").c_str());

    for(int i = 0; i < server_numbers.size(); i++) {
        int index = server_numbers[i];
        system((string("mkdir -p data/") + run_type + "/s" + to_string(index + 1)).c_str());
        thread_pool.emplace_back([index, run_type]() {
            machines[index].copy_from(string("project/sum.txt"), string("data/") + run_type + "/s" + to_string(index + 1));
        });
    }

    for(int i = 0; i < thread_pool.size(); i++){
        thread_pool[i].join();
    }

    return 0;
}

int main(int argc, char* argv[]){

    // string run_type = "ABD_NOF";

    if(argc < 2){
        return -1;
    }

    string run_type = argv[1];

    srand(time(nullptr));
    fill_zones_name();

    vector<int> all_servers = {0,1,2,3,4,5,6,7,8};

//    Machine m1("s4", "e2-micro", "us-central1-a");
//    cout << "the ip of the created machine is " << m1.get_external_ip() << endl;

    if(create_machines(9) != 0)
        return -1;

    write_setup_config();

    // clean_up();

    // get_pairwise_latency();

    // config_machines(false);

    manual_execute_command(all_servers, "cd project; make cleandb");
    
    vector<int> running_servers;
    cout << "Ready to run server" << endl;
    if(run_type[run_type.size() - 2] == '2'){
        running_servers = vector<int>({0,1,2,3,6,7,8});
    }
    else{
        running_servers = vector<int>({0,1,2,3,4,5,6,7,8});
    }
    thread(execute_server, running_servers).detach();
    
    thread(execute_metadata_server, all_servers).detach();

    sleep(5);
    cout << "Servers and metadata servers are running\n" << endl;//Press any key to stop them..." << endl;


    cout << "Running the controller...\n" << endl;
    string command = "cd /home/shahrooz/Desktop/PSU/Research/LEGOstore; ./Controller > cont_output_";
    command += run_type;
    command += ".txt 2>&1";
    system(command.c_str());

    sleep(2);

    //getchar();

    stop_server(all_servers);

    retrieve_sum(all_servers, run_type);

    // retrieve_data("ABD_2F");

    // manual_execute_command(running_servers, "sudo apt-get install wget; echo \"ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCdRtZWfwRgm6KxNXwrTIG+2axjp06FiM0NVBGMeVBQGVRj1D5TlsLPufmEXGJ6cpf4xT8doAN+H/d668diWJQSpE6Uo3BFVObSu7U3hXvuOBK126rBAuN4U2hMVtvinkASj9j21jYXcLKlMEc2tGhfRfzVNpflZu2rL67kizdm/6H7onlpXPUeXgXRWNvOJYUWLzdnhNPqv6PXA3pcPLpAumurTj8zwYQBAizKBXHFUB0C381v56NSHFCPpjZC9h3Ug2931jtGFYYdPVLpqvIdlPCet4aZgTtAE8RPMQSaXnxLWonerLH/MGNcdoI7fUaKtTT2jdhsempmKq3bKaRp shahrooz@s7\" >> .ssh/authorized_keys; echo \"-----BEGIN OPENSSH PRIVATE KEY-----\nb3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAABFwAAAAdzc2gtcn\nNhAAAAAwEAAQAAAQEAnUbWVn8EYJuisTV8K0yBvtmsY6dOhYjNDVQRjHlQUBlUY9Q+U5bC\nz7n5hFxienKX+MU/HaADfh/3euvHYliUEqROlKNwRVTm0ru1N4V77jgStduqwQLjeFNoTF\nbb4p5AEo/Y9tY2F3CypTBHNrRoX0X81TaX5Wbtqy+u5Is3Zv+h+6J5aVz1Hl4F0VjbziWF\nFi83Z4TT6r+j1wN6XDy6QLprq04/M8GEAQIsygVxxVAdAt/Nb+ejUhxQj6Y2QvYd1INvd9\nY7RhWGHT1S6aryHZTwnreGmYE7QBPETzEEml58S1qJ3qyx/zBjXHaCO31GirU09o3YbHpq\nZiqt2ymkaQAAA8h7DCQPewwkDwAAAAdzc2gtcnNhAAABAQCdRtZWfwRgm6KxNXwrTIG+2a\nxjp06FiM0NVBGMeVBQGVRj1D5TlsLPufmEXGJ6cpf4xT8doAN+H/d668diWJQSpE6Uo3BF\nVObSu7U3hXvuOBK126rBAuN4U2hMVtvinkASj9j21jYXcLKlMEc2tGhfRfzVNpflZu2rL6\n7kizdm/6H7onlpXPUeXgXRWNvOJYUWLzdnhNPqv6PXA3pcPLpAumurTj8zwYQBAizKBXHF\nUB0C381v56NSHFCPpjZC9h3Ug2931jtGFYYdPVLpqvIdlPCet4aZgTtAE8RPMQSaXnxLWo\nnerLH/MGNcdoI7fUaKtTT2jdhsempmKq3bKaRpAAAAAwEAAQAAAQEAiJD5DtRuLaEXDT8/\nGa3uP5Vtrn6ZnTQjsX4dWtgAV/0WnTSwBg80DAIV2swJqv+UXKyR2JyYS81gLLlNQWVe9i\nz8Gu8sTtehMr1RZuueqETCYm1jAQQMFvB98UO+3THCuxtzLyrkf0gZp3ybabIPqyLvnwgv\nrz/IAkx+Ve9Y5TKZkLvJUd2Y4i1rE9rsP9IeuoV2wYq1e2L8N9eDa0/jaCsKVdJqQkDVlF\n/MoSUhIsSAlkXDLihu1gfU4kFMRdFdkEglqgxakK2sPBAzQu/J2gsAVuri8th6qU+l61Kp\n2Bpkv/LYbofApMYchf2ozeJbigToXcMvGWgbuMZGJ481MQAAAIEAgpezlVt6mV02cDeabA\nk+aug6gPl+mlTvUbPqquoCwWQktuh4CTlXju/mHy4sGUPew/1JbSUhwupm8AVfq9J0merO\nrvul7A1qRr9V/B63vSKizTCLShPsiGPZwHjctbIqLjBfDXbhjQXTyyfpP60d+JlLW4embT\nJxUlJydipuCjAAAACBAM4lN3HmJTBejMqzbFEgrKptqlQWygWszyfn9WPkUEOMpVafdGSG\nOecMZREhPGvBT8GHhIg91/FfKwfLsiSI4RsxoPODH+44IHRRWTr9flGsaShiGLdgmvNokC\nUW0iSIi6bRSRRknkTT6ht9Gk7obOxEJUxTDjqINoqkFksMWOTNAAAAgQDDUBWzRPsEOsZ6\na2TRx1UrRQzmoXoW0SFjy5brZmMK7lY4HiSu2rMOeLCgl52EPvPHG0rjk7V+jiYrd0P3R3\n8FlsMqWLTJZjfXlzX3Rp0Bo44+v1fc93KslW7sSi/S8aBR9IVioW52CbB31fUThGPTfzFO\nfyk9iebGjsO1fy4eDQAAAAtzaGFocm9vekBzNwECAwQFBg==\n-----END OPENSSH PRIVATE KEY-----\" >> .ssh/id_rsa; chmod 600 .ssh/id_rsa");
    // manual_execute_command(running_servers, "wget https://kali.download/kali-images/kali-2020.3/kali-linux-2020.3-installer-netinst-amd64.iso");

    // manual_execute_command(running_metadata_servers, "sudo apt-get install bc");


	return 0;
}
