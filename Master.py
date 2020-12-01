import json
import socket
import time
import sys
import random
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import requests
import threading
import copy
import trace

def configure():
	with open(sys.argv[1]) as fp:
		f = json.load(fp)
	conf = f['workers']
	index = {}		# dictionary which has  indexes corresponding to workers 
	for i in range(len(conf)):
		index[conf[i]['worker_id']] = i
	return conf, index
	
def Task_launch(w_id, job_id, job_type, task):
	conf_lock.acquire()
	conf[w_id]['slots']=conf[w_id]['slots']-1	# decrement  no of free slots
	conf_lock.release()
	
	c = [{'worker_id' : i['worker_id'] , 'slots' : i['slots']} for i in conf]
	print("Config is now: ", c, "\n\n")
	
	if(w_id == 0):	 # To choose socket based on the respective worker's port					
		c, addr = socket1.accept()
	if(w_id == 1):
		c, addr = socket2.accept()
	if(w_id == 2):
		c, addr = socket3.accept()

	task['job_id'] = job_id	#To add job id&type (Map or reduce) 
	task['job_type'] = job_type
	task_lock.acquire()
	task_logs[task['task_id']] = [0, conf[w_id]['worker_id']]	#Adding to log files
	task_lock.release()
	msg = json.dumps(task)							
	c.send(msg.encode()) # sending task to the worker
	c.close()

#Scheduling by random way
def RANDOM(job_id, tasks, job_type):
	for task in tasks:
		conf_lock.acquire()						
		w_id = np.random.randint(0,3)				
		while(conf[w_id]['slots']==0):			
			conf_lock.release()
			time.sleep(1)
			conf_lock.acquire()
			w_id = np.random.randint(0,3)				
		print(task['task_id'], " allotted to Worker: ", conf[w_id]['worker_id'])
		conf_lock.release()
		Task_launch(w_id, job_id, job_type, task)	 #Sending to woker after initiating
		
#Scheduling by Round Robin way
def RR(job_id, tasks, job_type):
	for task in tasks:
		conf_lock.acquire()
		conf2 = copy.deepcopy(conf)
		conf2.sort(key = lambda x: x['worker_id'])
		w_id = 0
		while(conf2[w_id]['slots']==0):				
			w_id = (w_id+1)%3					
			conf_lock.release()
			time.sleep(1)
			conf_lock.acquire()
			conf2 = copy.deepcopy(conf)
			conf2.sort(key = lambda x: x['worker_id'])
		conf_lock.release()
		Task_launch(w_id, job_id, job_type, task)

#Scheduling by Least loaded method		
def LL(job_id, tasks, job_type):
	for task in tasks:
		conf_lock.acquire()
		conf2 = copy.deepcopy(conf)					
		conf2.sort(key=lambda x: x['slots'], reverse=True) # Sort based on free slots > desc
		while(conf2[0]['slots']==0):			
			conf_lock.release()
			time.sleep(1)			#if worker has free slot wait for 1sec			
			conf_lock.acquire()
			conf2 = copy.deepcopy(conf)
			conf2.sort(key=lambda x: x['slots'], reverse=True)
		w_id = index[conf2[0]['worker_id']]   #get the index of worker with more free alots
		print(task['task_id'], " allotted to Worker: ", conf[w_id]['worker_id'])
		conf_lock.release()
		Task_launch(w_id, job_id, job_type, task)			


def scheduling(job_id, tasks, job_type):	#scheduling based on command line argument				
	if(sys.argv[2] == "Random"):						
		RANDOM(job_id, tasks, job_type)
	elif(sys.argv[2] == "Round"):
		RR(job_id, tasks, job_type)
	elif (sys.argv[2] == 'Least'):
		LL(job_id, tasks, job_type)
	else:
		exit(0)
		
def Reduce():
	s = []			#to keep track of reduce job type			
	spool_lock.acquire()
	spool_copy = copy.deepcopy(spool)
	spool_lock.release()
	while(1):
		if(len(spool_copy)>0):
			for job_id, status in spool_copy.items():
				if(len(status[1]) == 0 and job_id not in s):	
					s.append(job_id)			
					scheduling(job_id, status[0], 'R')		
		time.sleep(1)	
		spool_lock.acquire()
		spool_copy = copy.deepcopy(spool)
		spool_lock.release()	
	
# Thread to address the job request
def Requests():
	global count
	while(1):
		try:
			c, addr = jsocket.accept()
		except:
			break
		k = c.recv(1024)	# To read the job request
		r = ""
		while k:							# If len(req) > 1024b
			r += k.decode()
			k = c.recv(1024)
		request = json.loads(r)					
		c.close()
		count_lock.acquire()
		count = count + 1
		count_lock.release()
		
		job_lock.acquire()
		job_logs[request['job_id']] = time.time()			# job start time
		job_lock.release()
		
		spool_lock.acquire()		#adding to spool
		spool[request['job_id']] = [request['reduce_tasks'], [i['task_id'] for i in request['map_tasks']]]
		spool_lock.release()
		
		scheduling(request['job_id'], request['map_tasks'], 'M')	
	print("\n----------------")

def update():              #updating the slots
	global count
	while(1):
		try:
			c,addr = j_socket.accept()
		except:
			break
		j = c.recv(1024).decode()			# get task completion information
		update = ""
		while(len(j)!=0):
			update=update + j
			j = c.recv(1024).decode()
		update = json.loads(update)
		
		task_lock.acquire()
		task_logs[update['task_id']][0] = update['end_time'] - update['start_time'] 	# add end time to logfile
		task_lock.release()

		w_id = index[update['w_id']]	 
		conf_lock.acquire()
		conf[w_id]['slots']=conf[w_id]['slots']+1 #increamenting the slots w.r.t worker after compl of task
		conf_lock.release()
		print(update['task_id'], " freed from Worker: ", conf[w_id]['worker_id'])
		w = [{'worker_id' : i['worker_id'] , 'slots' : i['slots']} for i in conf]
		
		print("Config is now: ", w, "\n\n")
	
		if(update['job_type'] == 'M'):						# If it was a map task
			
			spool_lock.acquire()
			spool[update['job_id']][1].remove(update['task_id'])	# remove job's map task list
			spool_lock.release()
			
		else:							# If it is a reduce task
			for task in spool[update['job_id']][0]:
				if task['task_id'] == update['task_id']:
					spool_lock.acquire()
					spool[update['job_id']][0].remove(task) # Remove from  job's reduce task list
					spool_lock.release()
					break
					
			if(len(spool[update['job_id']][0]) == 0):		# If no more r_tasks in resp job
												# Job completed
				print("\n\n")
				print("\t\t\t\t --------------- Job",update['job_id'],"completed-------------")
				print("\n\n")
				
				job_lock.acquire()
				job_logs[update['job_id']] = update['end_time'] - job_logs[update['job_id']]
				job_lock.release()
				
				spool_lock.acquire()
				spool.pop(update['job_id'])	# remove job from spool
				spool_lock.release()
				
				count_lock.acquire()
				count = count - 1
				count_lock.release()
				
		c.close()
	print("\n---")

# Initializing Configuration.
conf, index = configure()
conf_lock = threading.Lock()		
print(conf, "\n")

# creat Sockets
jsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
jsocket.settimeout(15.0)
jsocket.bind(("localhost", 5000))      # listens to job requests
jsocket.listen(1)

j_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
j_socket.settimeout(100.0)
j_socket.bind(("localhost", 5001))   # listens to job updates
j_socket.listen(3)

#launching the tasks on workers
socket1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
socket1.bind(("localhost", conf[0]['port']))   
socket1.listen(1)

socket2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
socket2.bind(("localhost", conf[1]['port']))
socket2.listen(1)

socket3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
socket3.bind(("localhost", conf[2]['port']))
socket3.listen(1)

# Initializing the logs to store time
job_logs = {}			
job_lock = threading.Lock()
task_logs = {}			
task_lock = threading.Lock()

count = 0
count_lock = threading.Lock()

spool = {}		#initialise the pool to keep track of map and reduce tasks
spool_lock = threading.Lock()

t1 = threading.Thread(target = Requests, name = "Request_thread")	
t2 = threading.Thread(target = update, name = "Update_thread")	
t3 = threading.Thread(target = Reduce, name = "Reduce_thread", daemon = True) 

t1.start()
t2.start()
t3.start()

t1.join()
t2.join()
t3.killed = True	#main thread terminated all the jobs completed


jsocket.close()
j_socket.close()
socket1.close()
socket2.close()
socket3.close()

if(sys.argv[2] == 'Random'):
	filename = "random.txt"
elif(sys.argv[2] == 'Round'):
	filename = "round.txt"
elif (sys.argv[2] == 'Least'):
	filename = "least.txt"
else:
	exit(0)
	
fp = open(filename, 'w')
fp.write(json.dumps(task_logs))
fp.write('\n')
fp.write(json.dumps(job_logs))
fp.close()
