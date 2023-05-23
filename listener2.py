from kazoo.client import KazooClient
import time

# Assume the name is given when you run this script
master_name = "minisql2"  # or minisql2, minisql3

zk = KazooClient(hosts='127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183')
zk.start()

def handle_instruction(data, stat, event):
    # Handle the instruction here...
    print("Received instruction: %s" % data.decode("utf-8"))

    # When done, notify the client
    if not zk.exists("/clients/"):
        zk.create("/clients/")
    if not zk.exists("/clients/" + master_name ):
        zk.create("/clients/" + master_name)

    done_path = "/clients/" + master_name + "/done"
    if zk.exists(done_path):
        print("Updating done node")
        # Update the done node instead of deleting and creating it
        zk.set(done_path, b"Done_update")
    else:
        print("Creating done node")
        zk.create(done_path, b"Done", ephemeral=True)

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
