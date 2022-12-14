import socket
import sys
import os
import hashlib
import threading
import time
import sys

startTime_0 = time.time()


##basic details
localIP = "127.0.0.1"
bufferSize = 1500
noc = 5

#############################################################################

###sockets
TCP_data_acceptor = []
TCP_data_acceptance_connection = []
for i in range(noc):
    TCPServerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
    TCPServerSocket.bind((localIP, 22000+i))
    TCPServerSocket.listen(1)
    TCP_data_acceptor.append(TCPServerSocket)

TCP_broadcast_responder = []
TCP_broadcast_response_connection = []
for i in range(noc):
    TCPServerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
    TCPServerSocket.bind((localIP, 23000+i))
    TCPServerSocket.listen(1)
    TCP_broadcast_responder.append(TCPServerSocket)


TCP_intial_receiver = []
for i in range(noc):
    TCPClientSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
    TCPClientSocket.bind(("127.0.0.1",40000+i))
    TCPClientSocket.connect((localIP,20000+i))
    TCP_intial_receiver.append(TCPClientSocket) 


UDP_request_sender = []
for i in range(noc):
    UDPClientSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    UDPClientSocket.bind(("127.0.0.1",41000+i))
    UDP_request_sender.append(UDPClientSocket)


for i in range(noc):
    connectionSocket, addr = TCP_data_acceptor[i].accept()   
    TCP_data_acceptance_connection.append(connectionSocket)
    

for i in range(noc):
    connectionSocket, addr = TCP_broadcast_responder[i].accept()   
    TCP_broadcast_response_connection.append(connectionSocket)


##############################################################################


##basic functions and global arrays

def not_found(list,n):
    ans_list = []
    Dict = {}
    for i in range(0,len(list)):
        Dict[list[i]]=1
    for i in range(0,n):
        if Dict.get(i) is None:
            ans_list.append(i)
    return ans_list

total_packets = [0 for i in range(noc)]
packets_received = []
for i in range(noc):
    Dict = {}
    packets_received.append(Dict)

# Rtt_table = [[] for i in range(116)]

logfile = "logfile_client.txt"
logstream = open(logfile,"a")
logstream.truncate(0)

################################################################################


##initial chunk distribution
def tcp_client(threadNumber):
    TCPClientSocket = TCP_intial_receiver[threadNumber]
    server_message = TCPClientSocket.recv(bufferSize)
    total_packets[threadNumber] = int(server_message.decode())
    TCPClientSocket.send("OK".encode())
    next_server_message = TCPClientSocket.recv(bufferSize)
    packet_count_client = int(next_server_message.decode())
    TCPClientSocket.send("OK".encode())
    for count in range(packet_count_client):
        packet_data = (TCPClientSocket.recv(bufferSize)).decode()
        # print("Chunk "+str(int(packet_data[:6]))+" received by client "+str(threadNumber))
        packets_received[threadNumber][int(packet_data[:6])] = packet_data
        TCPClientSocket.send("OK".encode())

##Requesting chunk 
def request_chunk(threadNumber):
    UDPClientSocket = UDP_request_sender[threadNumber]
    serverAddressPort = ("127.0.0.1", 21000+threadNumber)
    absent_chunks = not_found(list(packets_received[threadNumber].keys()),total_packets[threadNumber])
    for i in range(0,len(absent_chunks)):
        # start_rtt = time.time()
        msgFromClient = str(absent_chunks[i])
        bytesToSend = str.encode(msgFromClient)
        UDPClientSocket.sendto(bytesToSend, serverAddressPort)
        UDPClientSocket.recvfrom(bufferSize)
        accept_through_tcp(threadNumber)
        # end_rtt = time.time()
        # Rtt_table[absent_chunks[i]].append(end_rtt-start_rtt)
    bytesToSend = ('-1').encode()
    UDPClientSocket.sendto(bytesToSend, serverAddressPort)

##getting packet from tcp connection
def accept_through_tcp(threadNumber):
    connectionSocket = TCP_data_acceptance_connection[threadNumber]
    message = connectionSocket.recv(bufferSize).decode()        
    packets_received[threadNumber][int(message[:6])] = message
    logstream.write("Packet "+str(int(message[:6]))+" received by client "+str(threadNumber)+'\n')


#responding to broadcasts
def reply_to_broadcasts(threadNumber):
    while True:
        connectionSocket = TCP_broadcast_response_connection[threadNumber]
        message = connectionSocket.recv(bufferSize).decode() 
        if (message == '-1'):
            break
        packet_in_broadcast = int(message)  
        if packet_in_broadcast in packets_received[threadNumber].keys():
            connectionSocket.send(packets_received[threadNumber][packet_in_broadcast].encode())
        else:
            connectionSocket.send(str(-1).encode())


#threading implementation
threads =[]
startTime_1 = time.time()
noOfThreads = noc
for i in range(noOfThreads):   # for connection 1
    x = threading.Thread(target=tcp_client, args=(i,))
    threads.append(x)
    x.start()
for i in range(noOfThreads) :
    threads[i].join()
endTime_1 = time.time()
for i in range(noc):
    TCP_intial_receiver[i].close()

#############################


threads2 = []
startTime_2 = time.time()
for i in range(2*noOfThreads):   # for collecting chunk requests
    if i<noOfThreads:
        x = threading.Thread(target=request_chunk, args=(i,))
        threads2.append(x)
        x.start()
    else:
        x = threading.Thread(target=reply_to_broadcasts, args=(i-noOfThreads,))
        threads2.append(x)
        x.start()
for i in range(2*noOfThreads) :
    threads2[i].join()
endTime_2 = time.time()
for i in range(noc):
    TCP_data_acceptance_connection[i].close()
    TCP_data_acceptor[i].close()
for i in range(noc):
    TCP_broadcast_response_connection[i].close()
    TCP_broadcast_responder[i].close()

all_files = []
all_files_names = []
for i in range(noc):
    file_name = "small_file"+str(i)+".txt"
    all_files_names.append(file_name)
    file_name_stream = open(file_name,"a")
    all_files.append(file_name_stream)



for i in range(noc):
    all_files[i].truncate(0)
    for k in range(0,total_packets[i]):
        all_files[i].write(packets_received[i][k][6:])
    all_files[i].close()
    hash = hashlib.md5(open(all_files_names[i], 'r').read().encode()).hexdigest()
    print("MD5 hash of client "+str(i)+" is : "+hash)
print("Total time taken is : "+str(time.time()-startTime_0))
# print(Rtt_table)

logstream.close()
