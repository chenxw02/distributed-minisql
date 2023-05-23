from kazoo.client import KazooClient
import time

# Assume the name is given when you run this script
master_name = "minisql2"  # or minisql2, minisql3

zk = KazooClient(hosts='127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183')
zk.start()

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

    # When done, notify the client
    if not zk.exists("/clients/"):
        zk.create("/clients/")
    if not zk.exists("/clients/" + master_name ):
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


@zk.DataWatch("/servers/" + master_name + "/instructions")
def watch_node(data, stat, event):
    if event is not None and event.type == "CREATED":
        try:
            handle_instruction(data, stat, event)
        except Exception as e:
            print(f"An error occurred: {e}")

while True:
    # Keep your program running or the listener will stop
    time.sleep(1)
