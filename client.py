from kazoo.client import KazooClient
import random
import time
from functools import partial

zk = KazooClient(hosts='127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183')
zk.start()

servers = ["minisql1", "minisql2", "minisql3"]

completed_servers = []

def notify_done(server, data, stat, event):
    if event is not None and (event.type == "CREATED" or event.type == "CHANGED"):
        # 打印是哪个server完成了任务
        print(f"{server} has finished processing: {data.decode('utf-8')}")
        completed_servers.append(server)
        if zk.exists("/clients/" + server + "/done"):
            zk.delete("/clients/" + server + "/done")

def notify_fault_tolerance(server,data,stat,event):
    if event is not None and (event.type == "CREATED" or event.type == "CHANGED"):
        # 打印是哪个server完成了任务
        print(data.decode("utf-8"))
        #completed_servers.append(server)
        if zk.exists("/clients/" + server + "/fault_tolerance"):
            zk.delete("/clients/" + server + "/fault_tolerance")

# drop/insert/delete/update
def get_update_servers():
    # 从instruction中提取表名
    table_name = instruction.split(" ")[2]
    # 获取所有包含该表的服务器
    update_servers = []
    for server in servers:
        if zk.exists("/servers/" + server + "/tables/" + table_name):
            update_servers.append(server)
    if update_servers == []:
        print("no such table")
        return None
    else:
        return update_servers

def get_create_table_servers():
    # 从instruction中提取表名
    # 查找表最少的两个服务器
    min_table_num = 100
    min_table_server = []
    for server in servers:
        if zk.exists("/servers/" + server + "/tables"):
            table_num = len(zk.get_children("/servers/" + server + "/tables"))
            print(table_num)
            if table_num < min_table_num:
                min_table_num = table_num
                min_table_server.append(server)
    print(min_table_server)
    return min_table_server

def get_select_servers():
    # 从instruction中提取表名
    table_name = instruction.split(" ")[3]
    # 获取所有包含该表的服务器
    select_servers = []
    for server in servers:
        if zk.exists("/servers/" + server + "/tables/" + table_name):
            select_servers.append(server)
    # 数量大于1时，随机返回其中一个server
    if len(select_servers) > 1:
        select_servers = random.sample(select_servers, 1)
    else:
        print("no such table")
        return None


for server in servers:
    zk.DataWatch("/clients/" + server + "/done", partial(notify_done, server))
    zk.DataWatch("/clients/" + server + "/fault_tolerance", partial(notify_fault_tolerance, server))

while True:
    # Get user input
    instruction = input("Please enter your instruction: ")

    # Reset the completed servers list for this round
    completed_servers = []

    # Pick two random servers
    #selected_servers = random.sample(servers, 2)
    selected_servers = ['minisql1']

    for server in selected_servers:
        print("Sending instruction to %s" % server)

        # Make sure the parent node exists
        if not zk.exists("/servers/" + server):
            zk.create("/servers/" + server)

        # Delete the old instruction if it exists
        if zk.exists("/servers/" + server + "/instructions"):
            zk.delete("/servers/" + server + "/instructions")

        # Set the instruction and watch for a done signal
        zk.create("/servers/" + server + "/instructions/", instruction.encode("utf-8"), ephemeral=True)
        data, stat = zk.get("/servers/" + server + "/instructions")

        # zk.DataWatch("/clients/" + server + "/done", notify_done)

    # Wait for both masters to process the instruction
    while len(completed_servers) < 2:
        time.sleep(1)

    # Reset the completed servers list for the next round
    completed_servers = []
