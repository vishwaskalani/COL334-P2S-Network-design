import socket
import sys
import os
import hashlib
import threading
import time
import sys
import math

#basic details
localIP = "127.0.0.1"
bufferSize = 1500
noc = 5
total_packets = [0 for i in range(noc)]

#socket declaration
data_listning_sockets = []
for i in range(noc):
    UDPServerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    UDPServerSocket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 32768)
    UDPServerSocket.settimeout(1)
    UDPServerSocket.bind((localIP, 23000+i))
    data_listning_sockets.append(UDPServerSocket)

broadcast_listning_sockets = []
broadcast_listning_connection = []
for i in range(noc):
    TCPServerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
    TCPServerSocket.bind((localIP, 24000+i))
    TCPServerSocket.listen(1)
    broadcast_listning_sockets.append(TCPServerSocket)

UDP_client_sockets = []
for i in range(noc):
    UDPClientSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    UDPClientSocket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 32768)
    UDPClientSocket.bind(("127.0.0.1",30000+i))
    UDP_client_sockets.append(UDPClientSocket)

TCP_request_sender = []
for i in range(noc):
    TCPClientSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
    TCPClientSocket.bind(("127.0.0.1",32000+i))
    TCPClientSocket.connect((localIP,22000+i))
    TCP_request_sender.append(TCPClientSocket)

udp_broadcast_responder_sockets = []
for i in range(noc):
    UDPClientSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    UDPClientSocket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 32768)
    UDPClientSocket.settimeout(1)
    UDPClientSocket.bind(("127.0.0.1",35000+i))
    udp_broadcast_responder_sockets.append(UDPClientSocket)

for i in range(noc):
    TCPServerSocket = broadcast_listning_sockets[i]
    connectionSocket, addr = TCPServerSocket.accept() 
    broadcast_listning_connection.append(connectionSocket)

##########################################################################################

#helpful functions and declarations


logfile = "logfile_client.txt"
logstream = open(logfile,"a")
logstream.truncate(0)

packets_received = []
for i in range(noc):
    Dict = {}
    packets_received.append(Dict)

def not_found(list,n):
    ans_list = []
    Dict = {}
    for i in range(0,len(list)):
        Dict[list[i]]=1
    for i in range(0,n):
        if Dict.get(i) is None:
            ans_list.append(i)
    return ans_list

rtt_table = [[] for i in range(116)]

###########################################################################################

#transfer functions

def accept_chunk(threadNumber):
    UDPClientSocket = UDP_client_sockets[threadNumber]
    serverAddressPort = ("127.0.0.1", 20000+threadNumber)
    msgFromServer = UDPClientSocket.recvfrom(bufferSize) 
    total_packets[threadNumber] = int(msgFromServer[0].decode())
    msgFromClient = "Receiving"
    UDPClientSocket.sendto(msgFromClient.encode(), serverAddressPort)   
    msg2FromServer = UDPClientSocket.recvfrom(bufferSize) 
    packet_count_client = int(msg2FromServer[0].decode()) 
    UDPClientSocket.sendto(msgFromClient.encode(), serverAddressPort) 
    for i in range(packet_count_client):
        data_from_server = UDPClientSocket.recvfrom(bufferSize) 
        packets_received[threadNumber][int((data_from_server[0].decode())[:6])] = (data_from_server[0].decode())
        UDPClientSocket.sendto(msgFromClient.encode(), serverAddressPort)



def send_tcp_request(threadNumber):
    TCPClientSocket = TCP_request_sender[threadNumber]
    missing_packets = not_found(list(packets_received[threadNumber].keys()),total_packets[threadNumber]) 
    for i in range(len(missing_packets)):
        rtt_start = time.time()
        TCPClientSocket.send((str(missing_packets[i])).encode()) 
        get_data_udp(threadNumber)
        rtt_end = time.time()
        rtt_table[missing_packets[i]].append(rtt_end-rtt_start)
    TCPClientSocket.send(str(-1).encode())
    TCPClientSocket.close()



def get_data_udp(threadNumber):
    UDPServerSocket = data_listning_sockets[threadNumber]
    try:
        bytesAddressPair = UDPServerSocket.recvfrom(bufferSize)
    except:
        adr_ack = ("127.0.0.1",33000+threadNumber)
        UDPServerSocket.sendto("Unreceived".encode(),adr_ack)
        get_data_udp(threadNumber)
    msg_received = (bytesAddressPair[0]).decode()
    address = bytesAddressPair[1]
    UDPServerSocket.sendto("Received".encode(),address)
    packets_received[threadNumber][int(msg_received[:6])] = msg_received
    logstream.write("Packet "+str(int(msg_received[:6]))+" received by client "+str(threadNumber)+'\n')


#responding to broadcasts
def broadcast_listner(threadNumber):
    connectionSocket = broadcast_listning_connection[threadNumber]
    while True:              
        message = connectionSocket.recv(bufferSize).decode() 
        message = int(message)        
        if message != -1:
            if message in packets_received[threadNumber].keys():
                connectionSocket.send("I am having packet".encode())
                udp_broadcast_responder(threadNumber,message)
            else:
                connectionSocket.send("-1".encode())
        else: 
            logstream.write("connection closed with "+str(addr)+'\n')
            connectionSocket.close()
            break

def udp_broadcast_responder(threadNumber,packet_number):
    UDPClientSocket = udp_broadcast_responder_sockets[threadNumber]
    serverAddressPort = ("127.0.0.1", 25000+threadNumber)
    msgFromClient = packets_received[threadNumber][packet_number]
    UDPClientSocket.sendto(msgFromClient.encode(), serverAddressPort)
    try:
        ack = UDPClientSocket.recvfrom(bufferSize)
        if(ack[0].decode()=="Unreceived"):
            udp_broadcast_responder(threadNumber,packet_number)
    except:
        udp_broadcast_responder(threadNumber,packet_number)            



#threading implementation
threads =[]
startTime_1 = time.time()
noOfThreads = noc
for i in range(noOfThreads):   # for connection 1
    x = threading.Thread(target=accept_chunk, args=(i,))
    threads.append(x)
    x.start()    
for i in range(noOfThreads) :
    threads[i].join()
endTime_1 = time.time()
threads2 =[]
startTime_2 = time.time()
noOfThreads = noc
for i in range(2*noOfThreads):   # for connection 1
    if i<noOfThreads:
        x = threading.Thread(target=send_tcp_request, args=(i,))
        threads2.append(x)
        x.start()    
    else:
        x = threading.Thread(target=broadcast_listner, args=(i-noOfThreads,))
        threads2.append(x)
        x.start()  

for i in range(2*noOfThreads) :
    threads2[i].join()
endTime_2 = time.time()
for i in range(noc):
    broadcast_listning_sockets[i].close()

all_files = []
all_files_names = []
for i in range(noc):
    file_name = "smallFilePart2_client"+str(i)+".txt"
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
print("Total time taken is : "+str(time.time()-startTime_1))
logstream.close()
