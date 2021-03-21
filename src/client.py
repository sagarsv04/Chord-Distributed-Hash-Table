#!/usr/bin/env python3

import os
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
from thrift.Thrift import TException

# create your own handler
from chord import FileStore
from chord.ttypes import SystemException, RFileMetadata, RFile, NodeID



def choices_description():
   return """
Operation supports the following:
   1         - To WriteFile
   2         - To ReadFile
   3         - To FindSucc
   4         - To FindPred
   5         - To GetNodeSucc
"""


def get_choices():
   return [1, 2, 3, 4, 5]


def calculate_hash(str_data):

    str_hash = hashlib.sha256(str_data.encode('utf8')).hexdigest()
    return str_hash


def run_client(parser):

    args = parser.parse_args()
    if args.operation!=5  and args.file_name=="":
        print("Operation {0} requires file_name argument".format(args.operation))
        parser.print_help()
        sys.exit(os.EX_USAGE)
    else:

        transport = TSocket.TSocket(args.ip, args.port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = FileStore.Client(protocol)

        try:
            transport.open()
            if args.operation==1:
                # To WriteFile
                if DEBUG:
                    print("Client: {0}:{1} ".format(args.ip, args.port))
                    print("WriteFile: {0} ".format(args.file_name))
                rFile = RFile()
                rFile.content = "Any fool can write code that a computer can understand.\nGood programmers write code that humans can understand."
                meta = RFileMetadata(args.file_name, 0)
                rFile.meta = meta
                client.writeFile(rFile)
            elif args.operation==2:
                # To ReadFile
                if DEBUG:
                    print("Client: {0}:{1} ".format(args.ip, args.port))
                    print("ReadFile: {0} ".format(args.file_name))
                rFile =  client.readFile(args.file_name);
                print("File Name: {0} ".format(rFile.meta.filename))
                print("File Version: {0} ".format(rFile.meta.version))
                print("File Content: {0} ".format(rFile.content))
            elif args.operation==3:
                # To FindSucc
                if DEBUG:
                    print("Client: {0}:{1} ".format(args.ip, args.port))
                    print("FindSucc: {0} ".format(args.file_name))
                file_key = calculate_hash(args.file_name)
                succ_node = client.findSucc(file_key)
                print("Succ Node id: {0}".format(succ_node.id))
                print("Succ Node ip: {0}".format(succ_node.ip))
                print("Succ Node port: {0}".format(succ_node.port))
            elif args.operation==4:
                # To FindPred
                if DEBUG:
                    print("Client: {0}:{1} ".format(args.ip, args.port))
                    print("FindPred: {0} ".format(args.file_name))
                file_key = calculate_hash(args.file_name)
                pred_node = client.findPred(file_key)
                print("Pred Node id: {0}".format(pred_node.id))
                print("Pred Node ip: {0}".format(pred_node.ip))
                print("Pred Node port: {0}".format(pred_node.port))
            elif args.operation==5:
                # To GetNodeSucc
                if DEBUG:
                    print("Client: {0}:{1} ".format(args.ip, args.port))
                    print("GetNodeSucc:")
                first_node = client.getNodeSucc()
                print("First Node id: {0}".format(first_node.id))
                print("First Node ip: {0}".format(first_node.ip))
                print("First Node port: {0}".format(first_node.port))
            transport.close()
        except SystemException as sx:
            print("SystemException: {0}".format(sx))
        except TException as tx:
            print("TException: {0}".format(tx))

    return 0


def main():
    parser = argparse.ArgumentParser(description="Client", formatter_class=argparse.RawTextHelpFormatter, epilog=choices_description())
    parser.add_argument("operation", type=int, choices=get_choices(), help="Operation type")
    parser.add_argument("ip", type=str, help="ip address of server")
    parser.add_argument("port", type=int, help="port number of server")
    parser.add_argument("file_name", type=str, nargs="?", default="", help="file name")
    try:
        run_client(parser)
    except Exception as ex:
        print("Error: {0}".format(ex))

    return 0


if __name__ == '__main__':
    main()
