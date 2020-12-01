import socket
import sys
import json
import threading
import time

# Pool of tasks
pool = []					
lockk = threading.Lock()
	
def execution():
	while(True):
		for val in pool:
			cur_time = time.time()				# Check every task in pool
			if(cur_time >= val[1]['end_time']):	# If current_time > end_time set earlier, task complete
				val[1]['end_time'] = cur_time

				# print(val)
				# acquire lock here
				lockk.acquire()
				pool.remove(val)	
				# release lock here	
				lockk.release()
				# update
				sendUpdate(val[1]['job_id'], val[1]['task_id'], val[0], val[1]['job_type'], val[1]['start_time'], val[1]['end_time'])	


def connectionSocket(information):
	Sockett = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	Sockett.connect(("localhost", 5001))
	message = json.dumps(information)
	Sockett.send(message.encode())
	Sockett.close()



def sendUpdate(j_id, t_id, w_id, j_type, s_time, e_time):

	# DICTIONARY FOR MASTER
	info = {'job_id': j_id, 'job_type': j_type, 'task_id': t_id, 'w_id': w_id, 'start_time': s_time, 'end_time': e_time}
	# CONNECTION TO MASTER HERE
	connectionSocket(info)
	

def ProcessRequest(r,w_id):
	request = json.loads(r)
	request['start_time'] = time.time()
	request['end_time'] = request['start_time'] + request['duration']	# Add task completion time to request dict
	lockk.acquire()
	pool.append([w_id, request])				# Add request to the executing pool
	lockk.release()


def worker1(port, w_id):
	global pool
	while(1):
		taskLaunchSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		taskLaunchSocket.connect(("localhost", port))
		recieve = taskLaunchSocket.recv(1024)					# Read task
		requestMessage = ""
		while recieve:
			requestMessage += recieve.decode()
			recieve = taskLaunchSocket.recv(1024)
		if(requestMessage):
			ProcessRequest(requestMessage,w_id)
		else:
			break
		taskLaunchSocket.close()
		
	
thread1 = threading.Thread(target = worker1, args = (int(sys.argv[1]),int(sys.argv[2])))	
thread2 = threading.Thread(target = execution, name = "Another thread")			


thread1.start()
thread2.start()
thread1.join()
thread2.join()
