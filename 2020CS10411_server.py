import sys
import os
import socket
import hashlib
import threading
import time
import collections
import math


#basic details
localIP = "127.0.0.1"
bufferSize = 1500
noc = 5
inputFile = "A2_small_file.txt"
lock = threading.Lock()


#cache implementation
class LRUCache:
    def __init__(self, size):
        self.size = size
        self.cache = collections.OrderedDict()
    
    def get(self,packet_number):
        if packet_number not in self.cache:
            return -1
        else:
            packet = self.cache.pop(packet_number)
            self.cache[packet_number] = packet
            return packet

    def put(self,packet_number,packet):
        if packet_number not in self.cache:
            if len(self.cache)>=self.size:
                self.cache.popitem(last = False)
        else:
            self.cache.pop(packet_number)
        self.cache[packet_number] = packet

#cache of size of number of clients
cache_server = LRUCache(100)

## breaking the file in 1 kb chunks to distribute to the clients
m = open(inputFile, "r").read()
y = math.ceil(len(m)/1024)
# print(y)
client_packet_index = []
for i in range(0,noc):
    client_packet_index.append([f'{y:06d}'])
for i in range(0,noc):
    if i<y%noc:
        client_packet_index[i].append((y//noc)+1)
    else:
        client_packet_index[i].append(y//noc)
counter = 0
index = 0
while index<y :
    data = m[index*1024:(index+1)*1024]
    data =  f'{index:06d}'+data
    client_packet_index[counter].append(data)
    counter = (counter+1)%noc
    index += 1

logfile = "logfile_server.txt"
logstream = open(logfile,"a")
logstream.truncate(0)


global_update = [0 for i in range(noc)]

##sockets
TCP_server_sender = []
TCP_connection = []
for i in range(noc):
    TCPServerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
    TCPServerSocket.bind((localIP, 20000+i))
    TCPServerSocket.listen(1)
    TCP_server_sender.append(TCPServerSocket)
for i in range(noc):
    connectionSocket, addr = TCP_server_sender[i].accept()   
    TCP_connection.append(connectionSocket)

UDP_request_listener = []
for i in range(noc):
    UDPServerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    UDPServerSocket.bind((localIP, 21000+i))
    UDP_request_listener.append(UDPServerSocket)

TCP_data_sender = []
for i in range(noc):
    TCPClientSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
    TCPClientSocket.bind(("127.0.0.1",42000+i))
    TCPClientSocket.connect(("127.0.0.1",22000+i))
    TCP_data_sender.append(TCPClientSocket)

TCP_broadcast_sender = []
for i in range(noc):
    TCPClientSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
    TCPClientSocket.bind(("127.0.0.1",43000+i))
    TCPClientSocket.connect(("127.0.0.1",23000+i))
    TCP_broadcast_sender.append(TCPClientSocket)
    


##################################################################################


def tcp_server(threadNumber):
    connectionSocket = TCP_connection[threadNumber]   
    for i in range(len(client_packet_index[threadNumber])):
        connectionSocket.send((str(client_packet_index[threadNumber][i])).encode())
        connectionSocket.recv(bufferSize)


def accept_request(threadNumber):
    UDPServerSocket = UDP_request_listener[threadNumber]
    # print("UDP server listening to the requests")
    while(True):
        bytesAddressPair = UDPServerSocket.recvfrom(bufferSize)
        client_msg = bytesAddressPair[0]
        if(client_msg.decode()=='-1'):
            global_update[threadNumber]=1
            break
        packet_index = int(client_msg.decode())
        # acknowledge_through_tcp(threadNumber,packet_index)
        address = bytesAddressPair[1]
        clientMsg = "Request from client "+str(threadNumber)+" for packet {}".format(packet_index)          
        logstream.write(clientMsg+'\n')
        UDPServerSocket.sendto("Ok".encode(),address)
        lock.acquire()
        msg_to_be_sent = cache_server.get(packet_index)
        lock.release()
        if(msg_to_be_sent==-1):
            lock.acquire()
            broadcast(packet_index,threadNumber)
            lock.release()
            # print("sending after broadcast")
            # with lock:
            #     msg_to_be_sent = cache_server.get(packet_index)
            #     send_through_tcp(threadNumber,msg_to_be_sent)
        else:
            send_through_tcp(threadNumber,msg_to_be_sent)
    flag = True
    for k in range(noc):
        if global_update[k]==0:
            flag = False
            break
    lock.acquire()
    if flag == True:
        for i in range(noc):
            TCP_broadcast_sender[i].send('-1'.encode())
            TCP_broadcast_sender[i].close()
    lock.release()
    


def send_through_tcp(threadNumber,msg_to_be_sent):
    TCPClientSocket = TCP_data_sender[threadNumber]
    TCPClientSocket.send(msg_to_be_sent.encode())
    # server_message = TCPClientSocket.recv(26000)
    # first_message = (server_message).decode()
    # list_of_packets = first_message.split('\\')
    # for i in range(0,len(list_of_packets)):
    #     all_packets[threadNumber].append(int(list_of_packets[i][-3:]))
    #     packet_content[threadNumber][int(list_of_packets[i][-3:])] =  list_of_packets[i][:-3]       


def broadcast(packet_number,targetThread):
    for client in range(0,noc):
        TCPClientSocket = TCP_broadcast_sender[client]                
        TCPClientSocket.send(str(packet_number).encode())
        message = TCPClientSocket.recv(bufferSize).decode()     
        if message != '-1':
            cache_server.put(int(message[:6]),message)
            send_through_tcp(targetThread,message)
            break
        


#threading implementation
threads =[]
startTime_1 = time.time()
noOfThreads = noc
for i in range(noOfThreads):   # for connection 1
    x = threading.Thread(target=tcp_server, args=(i,))
    threads.append(x)
    x.start()    
endTime = time.time()
for i in range(noOfThreads) :
    threads[i].join()
endTime_1 = time.time()
for i in range(noc):
    TCP_connection[i].close()

#####################################################################



threads2 = []
startTime_2 = time.time()
for i in range(noOfThreads):   # for collecting chunk requests
    if i<noOfThreads:
        x = threading.Thread(target=accept_request, args=(i,))
        threads2.append(x)
        x.start()
for i in range(noOfThreads) :
    threads2[i].join()
endTime_2 = time.time()

logstream.close()