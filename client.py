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

# drop/insert/delete/update
def get_update_servers():
    # 从instruction中提取表名
    table_name = instruction.split(" ")[2]
    print(table_name)
    # 获取所有包含该表的服务器
    drop_table_servers = []
    for server in servers:
        if zk.exists("/servers/" + server + "/tables/" + table_name):
            drop_table_servers.append(server)
    return drop_table_servers

def get_create_table_servers():
    # 从instruction中提取表名
    # 查找表最少的两个服务器
    # 先看这个表是否已经存在
    table_name = instruction.split(" ")[2]
    for server in servers:
        if zk.exists("/servers/" + server + "/tables/" + table_name):
            return None
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

def validate_instruction(instruction):
    valid_starts = ["create table", "select", "drop table", "insert into", "delete from"]
    if not any(instruction.lower().startswith(vs) for vs in valid_starts):
        print("Error: Instruction must start with one of the following: 'create table', 'drop table', 'select', 'insert into', 'delete from'")
        return False
    return True

instruction_handlers = {
    "select": get_select_servers,
    "create table": get_create_table_servers,
    "drop table": get_update_servers,
    "insert into": get_update_servers,
    "delete from": get_update_servers,
}

while True:
    # Get user input
    instruction = input("Please enter your instruction: ")

    for instruction_start in instruction_handlers:
        if instruction.lower().startswith(instruction_start):
            selected_servers = instruction_handlers[instruction_start]()  # Call the corresponding function
            print(f"Selected servers: {selected_servers}")
            break
    else:
        print("Invalid instruction.")
        continue    

    # Pick two random servers
    selected_servers = random.sample(servers, 2)

    for server in selected_servers:
        print("Sending instruction to %s" % server)

        # Make sure the parent node exists
        if not zk.exists("/servers/" + server):
            zk.create("/servers/" + server)

        # Delete the old instruction if it exists
        if zk.exists("/servers/" + server + "/instructions"):
            zk.delete("/servers/" + server + "/instructions")

        if not zk.exists("/servers/" + server + "/tables"):
            zk.create("/servers/" + server + "/tables")

        # Set the instruction and watch for a done signal
        zk.create("/servers/" + server + "/instructions/", instruction.encode("utf-8"), ephemeral=True)
        data, stat = zk.get("/servers/" + server + "/instructions")

        # zk.DataWatch("/clients/" + server + "/done", notify_done)

    # Wait for both masters to process the instruction
    while len(completed_servers) < 2:
        time.sleep(1)

    # Reset the completed servers list for the next round
    completed_servers = []
