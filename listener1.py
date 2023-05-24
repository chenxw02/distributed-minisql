import threading
import socket

from kazoo.client import KazooClient
import time

# Assume the name is given when you run this script
master_name = "minisql1"  # or minisql2, minisql3

zk = KazooClient(hosts='127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183')
zk.start()

# socket
host = '127.0.0.1'
port = 8888
portNum = 0
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((host, port))
s.listen(3)

class Region_instance():
    region_socket = None
    region_socket_port = None

    @staticmethod
    def send_Instruction(instruction):
        if Region_instance.region_socket!=None:
            Region_instance.region_socket.sendall(instruction.encode())

    #向minisql（region）发送指令
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
        #提取表名
        table_name = data.decode("utf-8").split(" ")[2]
        print(table_name)
        #创建表
        if not zk.exists("/servers/" + master_name + "/tables"):
            zk.create("/servers/" + master_name + "/tables")
        if not zk.exists("/servers/" + master_name + "/tables/" + table_name):
            zk.create("/servers/" + master_name + "/tables/" + table_name)
            message = "table created"
        else :
            message = "table already exists"
    
    # 包含drop table
    if data.decode("utf-8").find("drop table") != -1:
        #提取表名
        table_name = data.decode("utf-8").split(" ")[2]
        print(table_name)
        #删除表
        if zk.exists("/servers/" + master_name + "/tables/" + table_name):
            zk.delete("/servers/" + master_name + "/tables/" + table_name)
            message = "table dropped"
        else :
            message = "table does not exist"




    if data.decode("utf-8").find("copy table") != -1:
        # 提取表名
        table_name = data.decode("utf-8").split(" ")[2]
        source_region_name = data.decode("utf-8").split(" ")[4]
        target_region_name = data.decode("utf-8").split(" ")[6]
        print(table_name)
        print(target_region_name)
        #TODO:进行table copy



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
            server_path = table_path + "/" + server_node
            server_data, _ = zk.get(server_path)

            # 如果server节点的数据包含offline_region_name，则将另一个server节点加入列表
            if offline_region_name in server_data.decode("utf-8"):
                other_server_node = "server1" if server_node == "server2" else "server2"
                other_server_path = table_path + "/" + other_server_node
                other_server_data, _ = zk.get(other_server_path)
                table_servers.append((table_name, other_server_data.decode("utf-8")))

        # 将当前table_name节点下符合条件的另一个server节点加入servers列表
        servers.extend(table_servers)

    return servers

#构建“copy table”的指令通过zookeeper传给source_server
def copy_instruction(source_server_name, target_server_name, table):
    instruction_path = "/servers/" + source_server_name + "/instructions"
    instruction_data = "copy table " + table + " from " + source_server_name + " to " + target_server_name

    # 检查instruction节点是否存在，存在则先删除再创建，不存在直接创建
    if zk.exists(instruction_path):
        zk.delete(instruction_path)
    zk.create(instruction_path, instruction_data.encode("utf-8"), ephemeral=True)




#这个函数当source_region_name为当前mastername的时候调用，把当前region的某一table通过zookeeper拷贝到target_region_name服务器中
def copy_table(target_region_name, source_region_name, table_name):

    #该函数被调用的时候只有当source_region_name为当前mastername的时候
    if source_region_name != master_name:
        print("copy table error")
        return

    # 发送指令给源区域
    Region_instance.send_Instruction(f"select * from {table_name}")

    # 等待并接收源区域的响应
    response = Region_instance.receive_Response()

    # 创建目标区域的表节点，并更新服务器节点内容
    table_properties = response[0]  # 表属性值
    table_values = response[1:]  # 表值
    table_properties_str = ','.join(table_properties)  # 表属性值字符串
    target_table_path = f"/servers/{target_region_name}/tables/{table_name}"
    if not zk.exists(target_table_path):
        zk.create(target_table_path, value=table_properties_str.encode("utf-8"))

    # 更新/table/xxx/server节点内容
    server1_path = f"/tables/{table_name}/server1"
    server2_path = f"/tables/{table_name}/server2"
    server1_value, _ = zk.get(server1_path)
    server2_value, _ = zk.get(server2_path)
    if server1_value.decode() != source_region_name:
        zk.set(server1_path, target_region_name.encode("utf-8"))
    else:
        zk.set(server2_path, target_region_name.encode("utf-8"))

    # 写入创建表的SQL指令到目标区域的指令节点
    instruction = f"create table {table_name} ({table_properties_str})"
    instruction_path = f"/servers/{target_region_name}/instructions"
    if zk.exists(instruction_path):
        zk.delete(instruction_path)
    zk.create(instruction_path, value=instruction.encode("utf-8"), ephemeral=True)

    # 等待指令执行完成
    done_path = f"/clients/{target_region_name}/done"
    while True:
        if zk.exists(done_path):
            done_data, _ = zk.get(done_path)
            if done_data == b"table created" or not zk.exists(instruction_path):
                break

    # 一条一条插入表的值到目标区域
    instruction_path = f"/servers/{target_region_name}/instructions"
    done_path = f"/clients/{target_region_name}/done"

    for row in table_values:
        values_str = ','.join(row)
        insert_instruction = f"insert into {table_name} values ({values_str})"

        # 创建instruction节点并写入指令
        if not zk.exists(instruction_path):
            zk.create(instruction_path)
        else:
            zk.delete(instruction_path)
            zk.create(instruction_path)

        zk.set(instruction_path, insert_instruction.encode("utf-8"))

        # 等待指令执行完成
        while True:
            if zk.exists(done_path):
                done_data, _ = zk.get(done_path)
                if done_data.decode() == "value inserted" or not zk.exists(instruction_path):
                    break




def insert_table_values(table_name, target_region_name, table_values):
    zk = KazooClient(hosts='127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183')
    zk.start()

    instruction_path = f"/servers/{target_region_name}/instructions"
    done_path = f"/clients/{target_region_name}/done"

    for row in table_values:
        values_str = ','.join(row)
        insert_instruction = f"insert into {table_name} values ({values_str})"

        # 创建instruction节点并写入指令
        if not zk.exists(instruction_path):
            zk.create(instruction_path)
        else:
            zk.delete(instruction_path)
            zk.create(instruction_path)

        zk.set(instruction_path, insert_instruction.encode("utf-8"))

        # 等待指令执行完成
        while True:
            if zk.exists(done_path):
                done_data, _ = zk.get(done_path)
                if done_data.decode() == "table created" or not zk.exists(instruction_path):
                    break

    zk.stop()
    zk.close()



@zk.DataWatch("/servers/" + master_name + "/instructions")
def watch_node(data, stat, event):
    if event is not None and event.type == "CREATED":
        try:
            handle_instruction(data, stat, event)
            Region_instance.send_Instruction(data.decode("utf-8"))
        except Exception as e:
            print(f"An error occurred: {e}")


@zk.DataWatch("/party/" + master_name)
def watch_party(data, stat, event):
    if event is not None and event.type == "CREATED":
        try:
            ##TODO：容错容灾处理
            print("party changes")
        except Exception as e:
            print(f"An error occurred: {e}")



##接收region的连接，构建region_instance与region通信
def add_Region(conn, addr):
    Region_instance.region_socket = conn
    Region_instance.region_socket_port = addr
    conn.sendall("You are Connected.".encode())
    region_name = conn.recv(1024).decode()
    print("region:" + region_name + " is connected")

while True:
    while True:
        conn, addr = s.accept()
        add_Region(conn, addr)
    # Keep your program running or the listener will stop
    #time.sleep(1)
