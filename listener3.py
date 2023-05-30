import threading
import socket
import random
import re

from kazoo.client import KazooClient
import time

# Assume the name is given when you run this script
master_name = "minisql3"  # or minisql2, minisql3

zk = KazooClient(hosts='127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183')
zk.start()

# socket
host = '127.0.0.1'
port = 8890
portNum = 0
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((host, port))
s.listen(3)


class Region_instance():
    region_socket = None
    region_socket_port = None

    # 向minisql（region）发送指令
    @staticmethod
    def send_Instruction(instruction):
        if Region_instance.region_socket != None:
            Region_instance.region_socket.sendall(instruction.encode())

    # 向获取minisql的响应，主要是select的时候使用，假设每一行中间用都好分隔
    @staticmethod
    def receive_Response():
        if Region_instance.region_socket != None:
            response = ""
            while True:
                data = Region_instance.region_socket.recv(1024).decode()
                response += data
                if "send end" in data:
                    break
            response = response.strip("send end").strip()  # 去除 "send end" 并移除首尾空格
            response_lines = response.split('\n')  # 按行分割响应内容
            response_tuples = [tuple(line.split(',')) for line in response_lines]  # 将每行内容转换为元组
            return response_tuples


def handle_instruction(data, stat, event):
    message = 'done!'
    # Handle the instruction here...
    print("Received instruction: %s" % data.decode("utf-8"))

    # 包含create table
    if data.decode("utf-8").find("create table") != -1:
        # 提取表名
        table_name = data.decode("utf-8").split(" ")[2]
        print(table_name)
        # 创建表
        if not zk.exists("/servers/" + master_name + "/tables"):
            zk.create("/servers/" + master_name + "/tables")

        if not zk.exists("/servers/" + master_name + "/tables/" + table_name):
            zk.ensure_path("/servers/" + master_name + "/tables/" + table_name)
            zk.ensure_path("/tables/" + table_name)
            if not zk.exists("/tables/" + table_name + "/" + master_name):
                zk.create("/tables/" + table_name + "/" + master_name, value=master_name.encode(("utf-8")))

            # Extract the column definitions using regular expressions
            match = re.match(r"create table \w+ \((.+)\);", data.decode("utf-8"))
            if match:
                column_definitions = match.group(1)
            else:
                print("Invalid SQL statement.")
            zk.create("/servers/" + master_name + "/tables/" + table_name + "/info",
                      value=column_definitions.encode("utf-8"))
            Region_instance.send_Instruction(data.decode("utf-8"))
            message = "table created"
        else:
            message = "table already exists"

    # 包含drop table
    if data.decode("utf-8").find("drop table") != -1:
        # 提取表名
        table_name = data.decode("utf-8").split(" ")[2]
        print(table_name)
        # 删除表
        if zk.exists("/servers/" + master_name + "/tables/" + table_name):
            zk.delete("/servers/" + master_name + "/tables/" + table_name, recursive=True)
        if zk.exists("/tables/" + table_name):
            zk.delete("/tables/" + table_name, recursive=True)
            Region_instance.send_Instruction(data.decode("utf-8"))
            message = "table dropped"
        else:
            message = "table does not exist"

    # 包含select
    if data.decode("utf-8").find("select") != -1:
        # 提取表名
        table_name = data.decode("utf-8").split(" ")[3]
        print(table_name)
        # 执行
        Region_instance.send_Instruction(data.decode("utf-8"))
        ret = Region_instance.receive_Response()
        message = '\n'.join([' '.join(item) for item in ret])

    # 包含 insert value
    if data.decode("utf-8").find("insert into") != -1:
        # 提取表名
        table_name = data.decode("utf-8").split(" ")[2]
        print(table_name)
        # 执行
        Region_instance.send_Instruction(data.decode("utf-8"))
        message = "insert value end"

    # 包含delete
    if data.decode("utf-8").find("delete") != -1:
        # 提取表名
        table_name = data.decode("utf-8").split(" ")[2]
        print(table_name)
        # 执行
        Region_instance.send_Instruction(data.decode("utf-8"))
        message = "delete value end"

    # 容错容灾的copy table
    if data.decode("utf-8").find("copy table") != -1:
        # 提取表名
        table_name = data.decode("utf-8").split(" ")[2]
        source_region_name = data.decode("utf-8").split(" ")[4]
        target_region_name = data.decode("utf-8").split(" ")[6]
        print(table_name)
        print(target_region_name)
        copy_table(target_region_name, source_region_name, table_name)
        return

    # When done, notify the client
    if not zk.exists("/clients/"):
        zk.create("/clients/")
    if not zk.exists("/clients/" + master_name):
        zk.create("/clients/" + master_name)

    done_path = "/clients/" + master_name + "/done"
    if zk.exists(done_path):
        # Update the done node instead of deleting and creating it
        print("Updating done node")
        # 发送message
        zk.set(done_path, message.encode("utf-8"))
    else:
        print("Creating done node")
        # 发送message
        zk.create(done_path, message.encode("utf-8"), ephemeral=True)

    # Make sure the instruction node exists before deleting it
    print("Deleting instruction node")
    if zk.exists(event.path):
        zk.delete(event.path)
    print("Done")


# 用于通知client关于容错容灾的情况，包括掉线，拷贝等
def notify_client_fault_tolerance(message):
    zk.ensure_path("/clients/" + master_name)

    done_path = "/clients/" + master_name + "/fault_tolerance"
    if zk.exists(done_path):
        # Update the done node instead of deleting and creating it
        print("Updating done node")
        # 发送message
        zk.set(done_path, message.encode("utf-8"))
    else:
        print("Creating done node")
        # 发送message
        zk.create(done_path, message.encode("utf-8"), ephemeral=True)
    return


# 找到掉线region对应的所有table的另一个copy所在的服务器名字，返回值以（tablename，servername）构成
def copy_master_name(offline_region_name):
    tables_path = "/tables"
    servers = []

    # 获取所有的table_name节点
    table_names = zk.get_children(tables_path)

    for table_name in table_names:
        table_path = tables_path + "/" + table_name
        table_servers = []

        # 获取当前table_name节点下的server节点
        server_nodes = zk.get_children(table_path)

        for server_node in server_nodes:
            if server_node != offline_region_name:
                table_servers.append((table_name, server_node))

        # 将当前table_name节点下符合条件的另一个server节点加入servers列表
        servers.extend(table_servers)

    print(servers)
    return servers


# 构建“copy table”的指令通过zookeeper传给source_server
def copy_instruction(source_server_name, target_server_name, table):
    instruction_path = "/servers/" + source_server_name + "/instructions"
    instruction_data = "copy table " + table + " from " + source_server_name + " to " + target_server_name

    # 检查instruction节点是否存在，存在则先删除再创建，不存在直接创建
    if zk.exists(instruction_path):
        zk.delete(instruction_path)
    zk.create(instruction_path, instruction_data.encode("utf-8"), ephemeral=True)

    notify_client_fault_tolerance(
        "fault happen in node " + master_name + ", disaster recovery is in progress, please wait...")


# 这个函数当source_region_name为当前mastername的时候调用，把当前region的某一table通过zookeeper拷贝到target_region_name服务器中
def copy_table(target_region_name, source_region_name, table_name):
    # 该函数被调用的时候只有当source_region_name为当前mastername的时候
    if source_region_name != master_name:
        print("copy table error")
        return

    # 发送指令给源区域
    Region_instance.send_Instruction(f"select * from {table_name}")

    # 等待并接收源区域的响应
    response = Region_instance.receive_Response()

    # 创建目标区域的表节点，并更新服务器节点内容

    table_values = response[1:]  # 表值

    info_path = "/servers/{}/tables/{}/info".format(source_region_name, table_name)
    table_properties_data, _ = zk.get(info_path)
    table_properties_str = table_properties_data.decode("utf-8")  # 表属性值字符串

    print(table_properties_str)

    # 更新/table/xxx/server节点内容
    children = zk.get_children(f"/tables/{table_name}")
    # 确保子节点数量为2
    if len(children) == 2:
        # 构建子节点路径
        server1_path = f"/tables/{table_name}" + "/" + children[0]
        server2_path = f"/tables/{table_name}" + "/" + children[1]
    server1_value, _ = zk.get(server1_path)
    server2_value, _ = zk.get(server2_path)
    if server1_value.decode() != source_region_name:
        zk.delete(server1_path, recursive=True)
        zk.create(f"/tables/{table_name}" + "/" + target_region_name, value=target_region_name.encode("utf-8"))

    else:
        zk.delete(server2_path, recursive=True)
        zk.create(f"/tables/{table_name}" + "/" + target_region_name, value=target_region_name.encode("utf-8"))

    # 写入创建表的SQL指令到目标区域的指令节点
    instruction = f"create table {table_name} ({table_properties_str});"
    instruction_path = f"/servers/{target_region_name}/instructions"
    if zk.exists(instruction_path):
        zk.delete(instruction_path)
    zk.create(instruction_path, value=instruction.encode("utf-8"), ephemeral=True)

    # 等待指令执行完成
    while True:
        if not zk.exists(instruction_path):
            break

    # 一条一条插入表的值到目标区域
    instruction_path = f"/servers/{target_region_name}/instructions"

    for row in table_values:
        values_str = ','.join(row)
        insert_instruction = f"insert into {table_name} values ({values_str});"

        # 创建instruction节点并写入指令
        if not zk.exists(instruction_path):
            zk.create(instruction_path, insert_instruction.encode("utf-8"))
        else:
            zk.delete(instruction_path)
            zk.create(instruction_path, insert_instruction.encode("utf-8"))

        # 等待指令执行完成
        while True:
            if not zk.exists(instruction_path):
                break

    notify_client_fault_tolerance(master_name + " copy table done")


def fault_tolerance(offline_region_name):
    # 获取当前已有的服务器名列表
    server_names = zk.get_children('/servers')

    # 随机打乱服务器名列表
    random.shuffle(server_names)

    # 调用 copy_master_name 函数获取需要进行容错容灾的表和copy table的源服务器的列表
    table_server_pairs = copy_master_name(offline_region_name)

    for table_name, source_server_name in table_server_pairs:
        # 选择一个不是目标服务器且不是全局变量 master_name 的源服务器
        target_server_name = None
        for server_name in server_names:
            if server_name != source_server_name and server_name != offline_region_name:
                target_server_name = server_name
                break

        if source_server_name is None:
            print(f"No available source server for table '{table_name}' and target server '{target_server_name}'.")
            continue

        # 调用 copy_instruction 函数进行容错容灾
        copy_instruction(source_server_name, target_server_name, table_name)

        # 等待容错容灾操作完成
        done_path = f"/clients/{source_server_name}/done"
        while True:
            if zk.exists(done_path):
                done_data, _ = zk.get(done_path)
                if done_data == b"copy table done":
                    break

    print("Fault tolerance and disaster recovery completed.")


@zk.DataWatch("/servers/" + master_name + "/instructions")
def watch_node(data, stat, event):
    if event is not None and event.type == "CREATED":
        try:
            handle_instruction(data, stat, event)
        except Exception as e:
            print(f"An error occurred: {e}")


party_flag = 0


@zk.ChildrenWatch("/party")
def watch_party_nodes(children):
    print("party changed")
    print(children)
    find_region = 0
    children = zk.get_children("/party")
    for child in children:
        node_path = "/party" + '/' + child
        node_data, _ = zk.get(node_path)
        if node_data.decode() == master_name:
            find_region = 1
            print(f"Node {child} contains master_name: {master_name}")

    if find_region == 0 and Region_instance.region_socket_port is not None:
        print(f"Node loss master_name: {master_name}")
        fault_tolerance(master_name)
        print("容错容灾end")


##接收region的连接，构建region_instance与region通信
def add_Region(conn, addr):
    Region_instance.region_socket = conn
    Region_instance.region_socket_port = addr
    conn.sendall("You are Connected.".encode())
    region_name = conn.recv(1024).decode()
    print("region:" + region_name + " is connected")


while True:
    conn, addr = s.accept()
    add_Region(conn, addr)
    # Keep your program running or the listener will stop
    # time.sleep(1)
