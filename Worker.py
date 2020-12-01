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



def sendUpdate(job_id, task_id, w_id, job_type, start_time, end_time):

	# DICTIONARY FOR MASTER
	info = {'job_id': job_id, 'job_type': job_type, 'task_id': task_id, 'w_id': w_id, 'start_time': start_time, 'end_time': end_time}
	# CONNECTION TO MASTER HERE
	connectionSocket(info)
	
def worker1(port, w_id):
	global pool
	while(1):
		taskLaunchSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		taskLaunchSocket.connect(("localhost", port))
		r = taskLaunchSocket.recv(1024)					# Read task
		req = ""
		while r:
			req += r.decode()
			r = taskLaunchSocket.recv(1024)
		if(req):
			request = json.loads(req)
			request['start_time'] = time.time()
			request['end_time'] = request['start_time'] + request['duration']	# Add task completion time to request dict
			lockk.acquire()
			pool.append([w_id, request])				# Add request to the executing pool
			lockk.release()
		else:
			break
		taskLaunchSocket.close()
		
	
t1 = threading.Thread(target = worker1, args = (int(sys.argv[1]),int(sys.argv[2])))	
t2 = threading.Thread(target = execution, name = "Another thread")			
								

t1.start()
t2.start()

t1.join()
t2.join()
