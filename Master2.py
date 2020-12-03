import sys
import json
import socket
import time
import random
import threading
import copy
import numpy as np



requests = []
finishRequests = {}

#toFinishMap = []
jobRequest = {}

countJobs = 0
countJobsLock = threading.Lock()
jobRequestLock = threading.Lock()
finishRequestsLock = threading.Lock()

configPath = sys.argv[1]
scheduleMethod = sys.argv[2]
f = open(configPath)
configuration = json.loads(f.read())
print(configuration)

requestSocket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
requestSocket.bind(("localhost", 5000))
requestSocket.listen(1)

workerSocket1 = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
workerSocket1.bind(("localhost", configuration['workers'][0]['port']))
workerSocket1.listen(1)

workerSocket2 = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
workerSocket2.bind(("localhost", configuration['workers'][1]['port']))
workerSocket2.listen(1)

workerSocket3 = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
workerSocket3.bind(("localhost", configuration['workers'][2]['port']))
workerSocket3.listen(1)


listenUpdateSocket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
listenUpdateSocket.bind(("localhost", 5001))
listenUpdateSocket.listen(1)


currentConfiguration = copy.deepcopy(configuration)
configurationLock = threading.Lock()



def acceptRequest():
	global countJobs
	while 1:
		connection , addr = requestSocket.accept()
		data = connection.recv(1024)
		# De-serializing data
		data_loaded = json.loads(data)
		connection.close()

		if(data_loaded):
			countJobsLock.acquire()
			countJobs+=1
			countJobsLock.release()
			jobRequestLock.acquire()
			jobRequest = data_loaded
			jobRequestLock.release()
			requests.append(jobRequest)
		print(jobRequest)
		print(countJobs)



def sendToWorker(chosenTask,workerNumber):
	if(workerNumber == 0):
		conn, addr = workerSocket1.accept()
	if(workerNumber == 1):
		conn, addr = workerSocket2.accept()
	if(workerNumber == 2):
		conn, addr = workerSocket3.accept()
	chosenTask['workerNumber']=workerNumber
	conn.send((json.dumps(chosenTask)).encode())
	conn.close()



def randomScheduling(chosenTask):
	numberOfWorkers = len(currentConfiguration['workers'])
	workerNumber = np.random.randint(0,numberOfWorkers)
	configurationLock.acquire()
	while currentConfiguration['workers'][workerNumber]['slots'] == 0:
		configurationLock.release()
		workerNumber = np.random.randint(0,numberOfWorkers)
		configurationLock.acquire()
	currentConfiguration['workers'][workerNumber]['slots']-=1
	configurationLock.release()
	print(chosenTask)
	sendToWorker(chosenTask,workerNumber)
	print('Task with task id ',chosenTask['task_id'],' is being scheduled on worker with id',currentConfiguration['workers'][workerNumber]['worker_id'])

def roundRobin(chosenTask):
	numberOfWorkers = len(currentConfiguration['workers'])
	workerNumber=0
	configurationLock.acquire()
	while currentConfiguration['workers'][workerNumber]['slots'] == 0:
		configurationLock.release()
		workerNumber = (workerNumber+1)%numberOfWorkers
		configurationLock.acquire()
	currentConfiguration['workers'][workerNumber]['slots']-=1
	configurationLock.release()
	print(chosenTask)
	sendToWorker(chosenTask,workerNumber)
	print('Task with task id ',chosenTask['task_id'],' is being scheduled on worker with id',currentConfiguration['workers'][workerNumber]['worker_id'])


def leastLoaded(chosenTask):
	numberOfWorkers = len(currentConfiguration['workers'])
	workerNumber=0
	minLoading = -1e9
	minLoadingIndex = 0
	configurationLock.acquire()
	while currentConfiguration['workers'][workerNumber]['slots'] == 0:
		noSlots = currentConfiguration['workers'][workerNumber]['slots']
		configurationLock.release()
		if minLoading<noSlots:
			minLoading=noSlots
			minLoadingIndex=workerNumber 
		workerNumber = (workerNumber+1)
		if workerNumber==numberOfWorkers:
			break;
		configurationLock.acquire()
	currentConfiguration['workers'][minLoadingIndex]['slots']-=1
	configurationLock.release()
	print(chosenTask)
	sendToWorker(chosenTask,workerNumber)
	print('Task with task id ',chosenTask['task_id'],' is being scheduled on worker with id',currentConfiguration['workers'][minLoadingIndex]['worker_id'])




def scanSchedule():
	global requests
	while(1):
		freeFlag=False
		configurationLock.acquire()
		for i in currentConfiguration['workers']:
			if i['slots']>0:
				freeFlag=True
				break;
		configurationLock.release()
		if freeFlag:
			# got atleast one worker free 
			# now get task and schedule that task
			chosenTask = {}
			rOver=False
			findTask = False
			for j in requests:
				finishRequestsLock.acquire()
				# mapper left so have to do mapper
				if(len(j['map_tasks'])):
					chosenTask = j['map_tasks'][0]
					#toFinishMap.append(j['map_tasks'][0])
					#finishRequestsLock.acquire()
					if j['job_id'] not in finishRequests.keys():
						finishRequests[j['job_id']]=[]
					finishRequests[j['job_id']].append(chosenTask)
					#finishRequestsLock.release()
					j['map_tasks'] = j['map_tasks'][1:]
					findTask=True
					break
				#schedule reducers
				elif len(finishRequests[j['job_id']])==0:
					print(j,j['reduce_tasks'],' IS J REDUCE TASKS')
					chosenTask = j['reduce_tasks'][0]
					j['reduce_tasks'] = j['reduce_tasks'][1:]
					findTask=True
					if(len(j['reduce_tasks'])==0):
						requests.remove(j)
						rOver = True
					break
				finishRequestsLock.release()
			if findTask:
				finishRequestsLock.release()		
			#if rOver:
				#requests = requests[1:]
			#print('seems free')
			if findTask and scheduleMethod == 'random':
				randomScheduling(chosenTask)
				pass
			elif findTask and scheduleMethod == 'RR':
				roundRobin(chosenTask)
				pass
			elif findTask and scheduleMethod == 'LL':
				leastLoaded(chosenTask)
				pass

def recieveUpdates():
	while 1:
		conn,addr = listenUpdateSocket.accept()
		data = conn.recv(1024)
		# De-serializing data
		data_loaded = json.loads(data)
		conn.close()
		print(data_loaded,' is what we recieve from worker')
		configurationLock.acquire()
		currentConfiguration['workers'][data_loaded['workerNumber']]['slots']+=1
		configurationLock.release()
		job_id = data_loaded['task_id'][:data_loaded['task_id'].find('_')]
		curr_task = {}
		curr_task['task_id']=data_loaded['task_id']
		curr_task['duration']=data_loaded['duration']
		curr_task['workerNumber']=data_loaded['workerNumber']
		if data_loaded['task_id'][data_loaded['task_id'].find('_')+1] == 'M':
			finishRequestsLock.acquire()
			#print(finishRequests[job_id],' is the finish requests list')
			#print(curr_task, ' is the task to remove')
			finishRequests[job_id].remove(curr_task)
			finishRequestsLock.release()

thread1 = threading.Thread(target = acceptRequest)
thread2 = threading.Thread(target = scanSchedule)
thread3 = threading.Thread(target = recieveUpdates)
thread1.start()
thread2.start()
thread3.start()
thread1.join()
thread2.join()
thread3.join()