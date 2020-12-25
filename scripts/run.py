#!/usr/bin/python3

import command
import subprocess
from subprocess import PIPE
import threading
import os
from time import sleep
from signal import signal, SIGINT

#Todo: Add command line parser

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


class Machine:
    machines = {}
    machines_lock = threading.Lock()

    existing_info = ''
    existing_info_lock = threading.Lock()

    def get_machine_list():
        zones = command.read_zones()
        threads = []
        for i, zone in enumerate(zones):
            name = "s" + str(i)
            threads.append(
                threading.Thread(target=(lambda m_name, m_type, m_zone: Machine(name=m_name, type=m_type, zone=m_zone)),
                                 args=[name, "e2-standard-2", zone]))
            threads[-1].start()
        # Machine(name=name, type="e2-standard-2", zone=zone)

        for thread in threads:
            thread.join()

        print("All machines are ready to use")
        return Machine.machines

    def execute_on_all(cmd):
        threads = []
        for machine in Machine.machines:
            threads.append(threading.Thread(target=Machine.execute, args=(Machine.machines[machine], cmd)))
            threads[-1].start()

        for thread in threads:
            thread.join()

    def dump_machines_ip_info():
        file = open('setup_config.json', 'w')
        file.write('{\n')

        for i in range(0, len(Machine.machines) - 1):
            machine_name = "s" + str(i)
            machine = Machine.machines[machine_name]
            machine_info = machine.__str__()
            for line in machine_info.split('\n')[:-1]:
                file.write('    ')
                file.write(line)
                file.write('\n')
            file.write('    ')
            file.write(machine_info.split('\n')[-1])
            file.write(',\n')

        machine_name = "s" + str(len(Machine.machines) - 1)
        machine = Machine.machines[machine_name]
        machine_info = machine.__str__()
        for line in machine_info.split('\n'):
            file.write('    ')
            file.write(line)
            file.write('\n')

        file.write('}')
        file.close()

    def run_all(): # if you call this method, you should not call run method anymore. It does not make sense to run twice!
        create_project_tar_file()
        threads = []
        for machine in Machine.machines:
            threads.append(threading.Thread(target=Machine.run, args=[Machine.machines[machine]]))
            threads[-1].start()

        for thread in threads:
            thread.join()

        delete_project_tar_file()

        print("All machines are running")

    def stop_all():
        threads = []
        for machine in Machine.machines:
            threads.append(threading.Thread(target=Machine.stop, args=[Machine.machines[machine]]))
            threads[-1].start()

        for thread in threads:
            thread.join()

    def clear_all():
        threads = []
        for machine in Machine.machines:
            threads.append(threading.Thread(target=Machine.clear, args=[Machine.machines[machine]]))
            threads[-1].start()

        for thread in threads:
            thread.join()

    def delete_all():
        threads = []
        for machine in Machine.machines:
            threads.append(threading.Thread(target=Machine.delete, args=[Machine.machines[machine]]))
            threads[-1].start()

        for thread in threads:
            thread.join()

    def gather_summary_all():
        threads = []
        for machine in Machine.machines:
            threads.append(threading.Thread(target=Machine.gather_summary, args=[Machine.machines[machine], "CAS_NOF"]))
            threads[-1].start()

        for thread in threads:
            thread.join()

    def gather_logs_all():
        threads = []
        for machine in Machine.machines:
            threads.append(threading.Thread(target=Machine.gather_logs, args=[Machine.machines[machine], "CAS_NOF"]))
            threads[-1].start()

        for thread in threads:
            thread.join()

    def __init__(self, **kwargs):  # Retrieve info if machine exists, create OW
        self.name = kwargs['name']
        self.type = "e2-standard-2"
        self.zone = kwargs['zone']

        Machine.existing_info_lock.acquire()
        Machine.existing_info = command.get_instances_info() if Machine.existing_info == '' else Machine.existing_info
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

        Machine.machines_lock.acquire()
        Machine.machines[self.name] = self
        Machine.machines_lock.release()

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
        return command.execute(self.name, self.zone, cmd)

    def copy_to(self, file, to_file):
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
                "sudo apt-get install -y build-essential autoconf automake libtool zlib1g-dev git protobuf-compiler pkg-config psmisc bc > /dev/null 2>&1")
            self.execute("git clone https://github.com/openstack/liberasurecode.git > /dev/null 2>&1")
            self.execute(
                "cd liberasurecode/; ./autogen.sh > /dev/null 2>&1; ./configure --prefix=/usr > /dev/null 2>&1; make -j 4> /dev/null 2>&1");
            self.execute("cd liberasurecode/; sudo make install > /dev/null 2>&1");
            self.copy_to("../lib/librocksdb.a", "")
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
        self.stop_server()
        self.stop_metadata_server()

    def gather_summary(self, run_name):
        self.execute("cd project/; ./summarize.sh " + run_name + " >sum.txt 2>&1")
        os.system("mkdir -p data/" + run_name + "/" + self.name)
        self.copy_from("project/sum.txt", "data/" + run_name + "/" + self.name + "/")

    def gather_logs(self, run_name):
        os.system("mkdir -p data/" + run_name + "/" + self.name)
        self.copy_from("project/*_output.txt", "data/" + run_name + "/" + self.name + "/")
        os.system("mkdir -p data/" + run_name + "/" + self.name + "/logs")
        self.copy_from("project/logs/*", "data/" + run_name + "/" + self.name + "/logs/")


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


def main():
    signal(SIGINT, keyboard_int_handler)

    Machine.get_machine_list()
    Machine.dump_machines_ip_info()
    Machine.run_all()

if __name__ == '__main__':
    main()
    print("Main thread goes to sleep")
    threading.Event().wait()
