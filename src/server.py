#!/usr/bin/env python3

import sys
import glob
import socket
import hashlib
import argparse
sys.path.append("gen-py")

DEBUG = False

if not DEBUG:
    sys.path.insert(0, glob.glob('/home/cs557-inst/thrift-0.13.0/lib/py/build/lib*')[0])
    pass

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

# create your own handler
from chord import FileStore
from chord.ttypes import SystemException, RFileMetadata, RFile, NodeID


def calculate_hash(str_data):
    # str_data = "127.0.0.1:9090"
    str_hash = hashlib.sha256(str_data.encode('utf8')).hexdigest()
    if DEBUG:
        print("Hash: {0}::{1}".format(str_data, str_hash))
    return str_hash


class StorageHandler(FileStore.Iface):

    def __init__(self, host_addr, port_num, node_hash):
        self.file_id = {}
        self.node_list = []
        self.port_num = port_num
        self.node_hash = node_hash
        self.host_addr = host_addr
        self.is_file_owned = False
        self.is_same_node = False
        self.current_node = NodeID('', '', 0)

    def writeFile(self, rFile):
        """
        Parameters:
         - rFile

        """
        if DEBUG:
            print("Inside writeFile ...")
        file_key = calculate_hash(rFile.meta.filename)
        file_node = self.findSucc(file_key)
        if DEBUG:
            print("Succ: {0}:{1}".format(file_node.ip, file_node.port))
        if file_node != self.current_node:
            self.is_file_owned = False
        elif self.is_same_node and file_node == self.current_node:
            self.is_file_owned = True

        if self.is_file_owned:
            self.createFile(rFile.meta.filename, rFile.content)
            self.is_file_owned = False
        else:
            exception = SystemException()
            exception.message = "This Server Node is not Finger Table Empty"
            raise exception
        return 0

    def readFile(self, filename):
        """
        Parameters:
         - filename

        """
        if DEBUG:
            print("Inside readFile ...")

        file_key = calculate_hash(filename)
        file_node = self.findSucc(file_key)
        if file_node != self.current_node:
            self.is_file_owned = False
        elif self.is_same_node and file_node == self.current_node:
            self.is_file_owned = True

        if self.is_file_owned:
            if file_key in self.file_id:
                if DEBUG:
                    print("File {0} exits ...".format(filename))
                # read the file and pass to func caller
                rFile = RFile()
                # read from disk
                with open(filename, "r") as fp:
                    rFile.content = fp.read()
                rFile.meta = self.file_id[file_key]
                return rFile
            else:
                exception = SystemException()
                exception.message = "File {0} is currently unavailable to this Server Node".format(filename)
                raise exception
            self.is_file_owned = False
        else:
            exception = SystemException()
            exception.message = "This Server Node is not the SUCCESSOR of file {0}".format(filename)
            raise exception
        return 0

    def setFingertable(self, node_list):
        """
        Parameters:
         - node_list

        """
        if DEBUG:
            print("Inside setFingertable ...")
        self.node_list = node_list
        self.current_node.ip = self.host_addr
        self.current_node.port = self.port_num
        self.current_node.id = self.node_hash
        return 0

    def findSucc(self, key):
        """
        Parameters:
         - key
        """
        if DEBUG:
            print("Inside findSucc ...")

        if len(self.node_list)>0:
            pred_node =  self.findPred(key)
            if pred_node.id == key:
                return pred_node
            elif pred_node == self.current_node:
                if self.is_same_node:
                    return self.current_node
                else:
                    succ_node = self.node_list[0]
                    return succ_node
            else:
                succ_node = self.get_server_node(pred_node.ip, pred_node.port, key, False)
                if succ_node == self.current_node:
                    self.is_file_owned = True
                else:
                    self.is_file_owned = False
                return succ_node
        else:
            exception = SystemException()
            exception.message = "Finger Table is Empty"
            raise exception
        return 0

    def check_node_in_range(self, key, first_node_id, second_node_id):
        """
        Parameters:
         - key
         - first_node_id
         - second_node_id
        """

        if DEBUG:
            print("Node: {0} in range {1} - {2}".format(key, first_node_id, second_node_id))

        key = int(key, 16)
        first_node_id = int(first_node_id, 16)
        second_node_id = int(second_node_id, 16)

        if first_node_id == second_node_id:
            print("Error: Invalid Id range check First == Second")
        elif first_node_id == key:
            if DEBUG:
                print("Node ID == First ID.")
            return True
        elif second_node_id == key:
            if DEBUG:
                print("Node ID == Second ID.")
            return True
        # check from here
        elif first_node_id > second_node_id:
            if key > first_node_id:
                if DEBUG:
                    print("First ID < Node ID.")
                return True
            elif key < second_node_id:
                if DEBUG:
                    print("Second ID > Node ID.")
                return True
            else:
                return False
        elif first_node_id < second_node_id:
            if key > first_node_id and key < second_node_id:
                if DEBUG:
                    print("First ID < Node ID < Second ID.")
                return True
            else:
                return False
        else:
            print("Error: Invalid Id range check")
        return False

    def get_nearest_node(self, key, is_pred):
        """
        Parameters:
         - key
         - is_pred
        """
        nearest_node = None
        is_in_range = False
        current_node_id = self.current_node.id
        if is_pred:
            # get preceding node
            if DEBUG:
                print("Getting Pred node of: {0}".format(key))
            for node in reversed(self.node_list):
                node_id = node.id
                is_in_range = self.check_node_in_range(node_id, current_node_id, key)
                if is_in_range:
                    nearest_node = node
                    break;
        else:
            # get succeding node
            if DEBUG:
                print("Getting Succ node of: {0}".format(key))
            for node in self.node_list:
                node_id = node.id
                is_in_range = self.check_node_in_range(node_id, current_node_id, key)
                if is_in_range:
                    nearest_node = node
                    break;
        # if nearest node not found then current node is nearest
        if not is_in_range:
            nearest_node = self.current_node
            if DEBUG:
                print("Using current node as nearest ...")
        return nearest_node

    def findPred(self, key):
        """
        Parameters:
         - key
        """
        if DEBUG:
            print("Inside findPred ...")
        pred_node = None
        first_node = self.getNodeSucc()
        if first_node != None:
            current_node_id = self.current_node.id
            first_node_id = first_node.id
            is_in_range = self.check_node_in_range(key, current_node_id, first_node_id)
            if is_in_range:
                self.is_file_owned = True
                if key == current_node_id:
                    self.is_same_node = True
                else:
                    self.is_same_node = False
                return self.current_node
            else:
                self.is_same_node = False
                self.is_file_owned = False
                nearest_node = self.get_nearest_node(key, True)
                if self.current_node == nearest_node:
                    pred_node = self.get_server_node(first_node.ip, first_node.port, key, True)
                else:
                    if nearest_node.ip == self.current_node.ip and nearest_node.port == self.current_node.port:
                        pred_node = nearest_node
                    else:
                        pred_node =  self.get_server_node(nearest_node.ip, nearest_node.port, key, True)
        else:
            if DEBUG:
                print("Returning current node ...")
            pred_node = self.current_node
        return pred_node

    def getNodeSucc(self):

        if DEBUG:
            print("Inside getNodeSucc ...")
        # it should be node before this node not 1st node
        server_node = None
        if len(self.node_list)>0:
            server_node = self.node_list[0]
        else:
            exception = SystemException()
            exception.message = "Finger Table is Empty"
            raise exception
        return server_node

    def createFile(self, filename, content):
        """
        Parameters:
         - filename
         - content

        """
        if DEBUG:
            print("Inside createFile ...")
        rFile = RFile()
        rFile_meta = RFileMetadata("", 0)
        rFile.meta = rFile_meta

        file_key = calculate_hash(filename)

        if file_key in self.file_id:
            if DEBUG:
                print("File {0} exits ...".format(filename))
            # write to the file and pass to func caller
            with open(filename, "w") as fp:
                fp.write(content)
            # store info about file in memory
            rFile_meta = self.file_id[file_key]
            rFile_meta.version += 1
            rFile.content = content
            rFile.meta = rFile_meta

        else:
            if DEBUG:
                print("File {0} does't exits ...".format(filename))
            # create the file and pass to func caller
            with open(filename, "w") as fp:
                fp.write(content)
            # store info about file in memory
            rFile_meta.filename = filename
            rFile.content = content
            rFile.meta = rFile_meta
        #  only keep meta info of file in dict
        self.file_id[file_key] = rFile.meta
        return 0

    def get_server_node(self, ip, port, key, is_pred):
        """
        Parameters:
         - ip
         - port
         - keys
         - is_pred
        """
        transport = TSocket.TSocket(ip, port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = FileStore.Client(protocol)

        server_node = None
        transport.open()
        if is_pred:
            if DEBUG:
                print("Connecting to Pred Node: {0}:{1}".format(ip, port))
            server_node = client.findPred(key)
        else:
            if DEBUG:
                print("Connecting to Succ Node: {0}:{1}".format(ip, port))
            server_node = client.getNodeSucc()
        transport.close()
        if DEBUG:
            print("Finished Connection to: {0}:{1}".format(ip, port))
        return server_node


def run_server(port_num):

    transport = TSocket.TServerSocket(port=port_num)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    host_addr = socket.gethostbyname(socket.gethostname())

    node_hash = calculate_hash("{0}:{1}".format(host_addr, port_num))

    handler = StorageHandler(host_addr, port_num, node_hash)
    processor = FileStore.Processor(handler)
    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
    print("Server IP: {0}".format(host_addr))
    print("Server Port: {0}".format(port_num))
    print("Server Hash: {0}".format(node_hash))
    print("Server is started...")
    server.serve()
    print("Done")

    return 0


def main():

    parser = argparse.ArgumentParser(description="Server")
    parser.add_argument("port", type=int, help="port number for server")
    args = parser.parse_args()
    port_num = args.port
    # port_num = 9090
    try:
        run_server(port_num)
    except Exception as ex:
        print("Error: {0}".format(ex))

    return 0


if __name__ == '__main__':
    main()
