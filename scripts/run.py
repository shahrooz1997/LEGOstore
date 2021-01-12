#!/usr/bin/python3

import command
import subprocess
from subprocess import PIPE
import threading
import os
from time import sleep
from signal import signal, SIGINT
import urllib.request
import argparse

# Please note that to use this script you need to install gcloud, login, and set the default project to Legostore.

#Todo: Add command line parser
def parse_args():
    parser = argparse.ArgumentParser(description="This application automatically runs the Legostore project on Google Cloud Platform")
    # parser.add_argument()

    # Manual run: Run the server on all the machines and initialize the metadata servers

    args = parser.parse_args()
    return args

# Reading zones to create the servers
def read_zones(zones_file_name='zones.txt'):
    zones_file = open(zones_file_name, "r")
    zones = []
    counter = 0
    for line in zones_file.readlines():
        # print(line)
        if (line[0] != "#" and line[0] != "\n"):
            zones.append(line[0:line.index(' ')]);
        if (line[0] == "\n"):
            counter += 1
            if (counter == 2):
                break
        else:
            counter = 0

    return zones

files_lock = threading.Lock()
def create_project_tar_file():
    files_lock.acquire()
    os.system("./copy2.sh")

def delete_project_tar_file():
    os.system("rm -rf project2.tar.gz project")
    files_lock.release()

def getting_librocksdb_download_link():
    # Source 1
    # headers = {'Referer': 'https://anonfiles.com/',
    #            'Host': 'anonfiles.com',
    #            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:84.0) Gecko/20100101 Firefox/84.0'}
    # url = 'https://anonfiles.com/P7Tet126p1/librocksdb_a'
    # req = urllib.request.Request(url, headers=headers)
    # page = urllib.request.urlopen(req).read().decode('utf-8')
    # link = page[page.find('download-url'):page.find('>', page.find('download-url'))]
    # link = link[link.find('href'):]
    # link = link[link.find('"') + 1:link.find('"', link.find('"') + 1)]

    # Source 2
    os.system("rm -f /tmp/LEGOSTORE_LIBROCKSDB_LINK.txt")
    os.system("curl -Lqs \"https://www.mediafire.com/file/bs5ob9lw3urrm6g/librocksdb.a/file\" | grep \"href.*download.*media.*\" | tail -1 | cut -d '\"' -f 2 > /tmp/LEGOSTORE_LIBROCKSDB_LINK.txt")

    with open("/tmp/LEGOSTORE_LIBROCKSDB_LINK.txt") as link_file:
        link = link_file.readlines()[0][:-1]

    return link

class Machine:

    existing_info = ''
    existing_info_lock = threading.Lock()

    def get_machine_list(number_of_machines=9):
        def create_machine_thread_helper(machines, m_name, m_type, m_zone):
            machines[m_name] = Machine(name=m_name, type=m_type, zone=m_zone)
        zones = command.read_zones()
        threads = []
        machines = {} # [None] * number_of_machines
        for i, zone in enumerate(zones):
            if(i >= number_of_machines):
                break;
            name = "s" + str(i)
            threads.append(
                # threading.Thread(target=(lambda m_name, m_type, m_zone: Machine(name=m_name, type=m_type, zone=m_zone)),
                #                  args=[name, "e2-standard-2", zone]))
                threading.Thread(target=create_machine_thread_helper, args=[machines, name, "e2-standard-2", zone]))

            threads[-1].start()

        for thread in threads:
            thread.join()

        print("All machines are ready to use")
        return machines

    def execute_on_all(machines, cmd):
        threads = []
        for machine in machines:
            threads.append(threading.Thread(target=machines[machine].execute, args=[cmd]))
            threads[-1].start()

        for thread in threads:
            thread.join()

    def copy_to_all(machines, file, to_file):
        threads = []
        for machine in machines:
            threads.append(threading.Thread(target=machines[machine].copy_to, args=[file, to_file]))
            threads[-1].start()

        for thread in threads:
            thread.join()

    def dump_machines_ip_info(machines):
        file = open('setup_config.json', 'w')
        file.write('{\n')

        for i in range(0, len(machines) - 1):
            machine_name = "s" + str(i)
            machine = machines[machine_name]
            machine_info = machine.__str__()
            for line in machine_info.split('\n')[:-1]:
                file.write('    ')
                file.write(line)
                file.write('\n')
            file.write('    ')
            file.write(machine_info.split('\n')[-1])
            file.write(',\n')

        machine_name = "s" + str(len(machines) - 1)
        machine = machines[machine_name]
        machine_info = machine.__str__()
        for line in machine_info.split('\n'):
            file.write('    ')
            file.write(line)
            file.write('\n')

        file.write('}')
        file.close()

        os.system("cp setup_config.json ../config/auto_test/datacenters_access_info.json")

    def get_pairwise_latencies(machines):
        os.system("rm -f pairwise_latencies/latencies.txt")

        threads = []
        for machine in machines:
            threads.append(threading.Thread(target=machines[machine].get_latencies, args=[machines]))
            threads[-1].start()

        for thread in threads:
            thread.join()

        for i in range(0, len(machines)):
            os.system("cat pairwise_latencies/latencies_from_server_" + str(i) + ".txt >> pairwise_latencies/latencies.txt")

        print("Pairwise latencies are ready in pairwise_latencies/latencies.txt")

    def run_all(machines): # if you call this method, you should not call run method anymore. It does not make sense to run twice!
        create_project_tar_file()
        threads = []
        for machine in machines:
            threads.append(threading.Thread(target=machines[machine].run, args=[]))
            threads[-1].start()

        for thread in threads:
            thread.join()

        delete_project_tar_file()

        print("All machines are running")

    def stop_all(machines):
        threads = []
        for machine in machines:
            threads.append(threading.Thread(target=machines[machine].stop, args=[]))
            threads[-1].start()

        for thread in threads:
            thread.join()

    def clear_all(machines):
        threads = []
        for machine in machines:
            threads.append(threading.Thread(target=machines[machine].clear, args=[]))
            threads[-1].start()

        for thread in threads:
            thread.join()

    def delete_all(machines):
        threads = []
        for machine in machines:
            threads.append(threading.Thread(target=machines[machine].delete, args=[]))
            threads[-1].start()

        for thread in threads:
            thread.join()

    def gather_summary_all(machines):
        threads = []
        for machine in machines:
            threads.append(threading.Thread(target=machines[machine].gather_summary, args=["CAS_NOF"]))
            threads[-1].start()

        for thread in threads:
            thread.join()

    def gather_logs_all(machines):
        threads = []
        for machine in machines:
            threads.append(threading.Thread(target=machines[machine].gather_logs, args=["CAS_NOF"]))
            threads[-1].start()

        for thread in threads:
            thread.join()

    def __init__(self, **kwargs):  # Retrieve info if machine exists, create OW
        self.name = kwargs['name']
        self.type = "e2-standard-2"
        self.zone = kwargs['zone']

        Machine.existing_info_lock.acquire()
        info_line = ''
        for line in Machine.existing_info.split('\n'):
            line = " ".join(line.split())
            if self.name == line.split(' ')[0]:
                info_line = line
                break

        if info_line == '':
            Machine.existing_info = command.get_instances_info()
            for line in Machine.existing_info.split('\n'):
                line = " ".join(line.split())
                if self.name == line.split(' ')[0]:
                    info_line = line
                    break
        Machine.existing_info_lock.release()

        if info_line == '':
            command.start_server(self.name, self.type, self.zone)
            print("Machine " + self.name + " created")
            sleep(20)  # Wait for the machine to boot up
            Machine.existing_info_lock.acquire()
            Machine.existing_info = command.get_instances_info()
            for line in Machine.existing_info.split('\n'):
                line = " ".join(line.split())
                if self.name == line.split(' ')[0]:
                    info_line = line
                    break
            Machine.existing_info_lock.release()

        if info_line == '':
            raise Exception("Error in creating server and setting infor for " + name)

        self.init_config()

        self.zone = info_line.split(' ')[1]
        self.type = info_line.split(' ')[2]
        self.internal_ip = info_line.split(' ')[3]
        self.external_ip = info_line.split(' ')[4]

        print("Machine " + self.name + " is ready to use")

    def __str__(self):
        ret = ""
        ret += '"'
        ret += self.name[1]
        ret += '": {\n'
        ret += '    "metadata_server": {\n'
        ret += '        "host": "'
        ret += self.external_ip
        ret += '",\n'
        ret += '        "port": "30000"\n    },\n    "servers": {\n        "1": {\n            "host": "'
        ret += self.external_ip
        ret += '",\n            "port": "10000"\n        }\n    }\n}'
        return ret

    def execute(self, cmd):
        # print("executing " + cmd + " on server " + self.name)
        stdout, stderr = command.execute(self.name, self.zone, cmd)
        # print("output: " + stdout + " || " + stderr)
        return stdout, stderr

    def copy_to(self, file, to_file=""):
        command.scp_to(self.name, self.zone, file, to_file)

    def copy_from(self, file, to_file):
        command.scp_from(self.name, self.zone, file, to_file)

    def disk_resize(self, size_gb=20):
        command.disk_resize(self.name, self.zone, size_gb)

    def init_config(self):
        stdout, stderr = self.execute("ls ../init_config_done")
        if stdout.find("No such file or directory") != -1 or stderr.find("No such file or directory") != -1:
            print("initializing machine ", self.name)
            self.execute(
                "sudo apt-get install -y build-essential autoconf automake libtool zlib1g-dev git protobuf-compiler pkg-config psmisc bc aria2 >/dev/null 2>&1")
            self.execute("git clone https://github.com/openstack/liberasurecode.git >/dev/null 2>&1")
            self.execute(
                "cd liberasurecode/; ./autogen.sh >/dev/null 2>&1; ./configure --prefix=/usr >/dev/null 2>&1; make -j 4 >/dev/null 2>&1");
            self.execute("cd liberasurecode/; sudo make install >/dev/null 2>&1");

            # self.copy_to("../lib/librocksdb.a", "")
            link = getting_librocksdb_download_link()
            # self.execute("aria2c -x 5 -s 5 " + link + ">/dev/null 2>&1")
            self.execute("link=$(curl -Lqs \"https://www.mediafire.com/file/bs5ob9lw3urrm6g/librocksdb.a/file\" | grep \"href.*download.*media.*\" | tail -1 | cut -d '\"' -f 2); aria2c -x 5 -s 5 $link")
            stdout, stderr = self.execute("ls librocksdb.a.aria2")
            if not (stdout.find("No such file or directory") != -1 or stderr.find("No such file or directory") != -1):
                print("Error in downloading librocksdb.a on server " + self.name)
            else:
                self.execute("sudo mv librocksdb.a ../")
                self.execute("sudo touch ../init_config_done")

    def config(self, make_clear=False, clear_all=False):
        if clear_all:
            self.clear()
        print("sending and compiling the project on machine " + self.name)
        project_tar_file_created = False
        if not os.path.exists("project2.tar.gz"):
            create_project_tar_file()
            project_tar_file_created = True
        self.copy_to("project2.tar.gz", "")
        if project_tar_file_created:
            delete_project_tar_file()

        self.execute("tar -xzf project2.tar.gz")
        stdout, stderr = self.execute("ls project/lib/librocksdb.a")
        if stdout.find("No such file or directory") != -1 or stderr.find("No such file or directory") != -1:
            self.execute("cp ../librocksdb.a ./project/lib/")

        if make_clear:
            self.execute("cd project/; make cleanall; make -j 4 > /dev/null 2>&1")
        else:
            self.execute("cd project/; make -j 4 > /dev/null 2>&1")

    def get_latencies(self, machines):
        self.copy_to("./myping", "")
        command = "./myping " + self.name[1:] + " "
        for i in range(0, len(machines)):
            machine_name = "s" + str(i)
            machine = machines[machine_name]
            command += machine.external_ip + " "

        command = command[:-1]
        self.execute(command)
        os.system("rm -f pairwise_latencies/latencies_from_server_" + self.name[1:] + ".txt")
        self.copy_from("latencies_from_server_" + self.name[1:] + ".txt", "pairwise_latencies")


    def clear(self):
        self.execute("rm -rf *")

    def make_clear(self):
        self.execute("cd project/; make cleanall")

    def delete(self):
        command.delete_server(self.name, self.zone)

    def run(self):
        self.stop()
        self.config()
        self.execute("cd project/; make cleandb >/dev/null 2>&1")
        server_thread = threading.Thread(target=Machine.execute, args=[self, "cd project/; ./Server " + \
                                                                       self.internal_ip + " 10000 db " + self.internal_ip + " 30000 >server_output.txt 2>&1"])
        server_thread.daemon = True
        server_thread.start()
        metadata_server_thread = threading.Thread(target=Machine.execute,
                                                  args=[self, "cd project/; ./Metadata_Server " + \
                                                        self.internal_ip + " 30000 >metadata_output.txt 2>&1"])
        metadata_server_thread.daemon = True
        metadata_server_thread.start()
        while (True):
            stdout, stderr = self.execute("ps aux | grep Server | wc -l")
            if int(stdout) > 7:
                raise Exception("Machine is already running")
            if int(stdout) > 5:
                break;
            sleep(1)

        print("server and metadata_server are running on " + self.name)

    def stop_server(self):
        self.execute("killall Server")

    def stop_metadata_server(self):
        self.execute("killall Metadata_Server")

    def stop(self):
        self.execute("killall Server Metadata_Server Controller")
        # self.stop_server()
        # self.stop_metadata_server()

    def gather_summary(self, run_name):
        self.execute("cd project/; ./summarize.sh " + run_name + " >sum.txt 2>&1")
        os.system("mkdir -p data/" + run_name + "/" + self.name)
        self.copy_from("project/sum.txt", "data/" + run_name + "/" + self.name + "/")

    def gather_logs(self, run_name):
        os.system("mkdir -p data/" + run_name + "/" + self.name)
        self.copy_from("project/*_output.txt", "data/" + run_name + "/" + self.name + "/")
        os.system("mkdir -p data/" + run_name + "/" + self.name + "/logs")
        self.copy_from("project/logs/*", "data/" + run_name + "/" + self.name + "/logs/")


# Signal handlers
keyboard_int_handler_called = False
def keyboard_int_handler(signal_received, frame):
    global keyboard_int_handler_called

    if keyboard_int_handler_called:
        print("Please Wait...")
    else:
        keyboard_int_handler_called = True
        print("Please wait while I am stopping all the machines and gathering the logs...")
        Machine.stop_all()
        Machine.gather_summary_all()
        Machine.gather_logs_all()
        exit(0)


class Controller(Machine):
    def __init__(self):
        # Machine.__init__(self, name='s7-cont', zone='us-west2-a')
        # self.cont_machine = None
        self.created = False
        self.add_access_done = False
        def create_controller(self):
            Machine.__init__(self, name='s7-cont', zone='us-west2-a')
            # self.cont_machine = Machine(name='s7-cont', zone='us-west2-a')
        self.creator_thread = threading.Thread(target=create_controller, args=[self])
        self.creator_thread.start()

    def is_created(self):
        if(self.created):
            return True
        self.creator_thread.join()
        self.created = True
        return self.created

    def add_access_to_clients(self, clients):
        self.is_created()
        stdout, stderr = self.execute("ls ../access_added")
        if stdout.find("No such file or directory") != -1 or stderr.find("No such file or directory") != -1:
            print("Adding access to the clients from server ", self.name)
            self.execute("ssh-keygen -q -t rsa -N '' <<< \"\"$'\n'\"y\" 2>&1 >/dev/null")
            stdout, stderr = self.execute("cat .ssh/id_rsa.pub")
            Machine.execute_on_all(clients, "echo '" + stdout + "' >>.ssh/authorized_keys")
            self.execute("sudo touch ../access_added")
        self.add_access_done = True

    def run(self, clients):
        self.is_created()
        if not self.add_access_done:
            self.add_access_to_clients(clients)
        self.stop()
        self.config()
        self.execute("cd project/; make cleandb >/dev/null 2>&1")
        print("Controller is running on " + self.name)
        self.execute('cd project/; ./Controller >controller_output.txt 2>&1')

    def gather_summary(self, run_name):
        pass

    def gather_logs(self, run_name):
        os.system("mkdir -p data/" + run_name + "/" + self.name)
        self.copy_from("project/*_output.txt", "data/" + run_name + "/" + self.name + "/")

def make_sure_project_can_be_built():
    if os.system("cd ..; make -j 9 >/dev/null 2>&1") != 0:
        print("Compile ERROR")
        os.system("cd ..; make")
        exit(-1)

def main():
    # signal(SIGINT, keyboard_int_handler)
    make_sure_project_can_be_built()

    controller = Controller()
    machines = Machine.get_machine_list()
    Machine.dump_machines_ip_info(machines)
    Machine.get_pairwise_latencies(machines)
    Machine.run_all(machines)
    controller.run(machines)
    machines[controller.name] = controller
    print("Project execution finished.\nPlease wait while I am stopping all the machines and gathering the logs...")
    os.system("rm -rf /home/shahrooz/Desktop/PSU/Research/LEGOstore/scripts/data/CAS_NOF")
    Machine.stop_all(machines)
    Machine.gather_summary_all(machines)
    Machine.gather_logs_all(machines)

if __name__ == '__main__':
    # print(can_project_be_built())

    main()
    # print("Main thread goes to sleep")
    # threading.Event().wait()
