#!/bin/bash +vx

# To run server generated by thrift compiler
# python3 src/PythonServer.py $1

# To run multiple servers with list of ip:port info in a file
echo "Please enter nodes file full path."
read input_file

# cat $input_file | sed -e "s/:/ /" | while read ip port; do echo $ip "Hey $port"; done

cat $input_file | sed -e "s/:/ /" | while read ip port;
do
  ( echo "Starting server at $ip:$port ..."
  xterm -title "Server $port" -e "python3 ./src/server.py $port" &);
done

echo "Finished starting servers ..."
