import sys
import json
import socket
import time
import random
import threading
import copy
import numpy as np



requestSocket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
requestSocket.bind(("localhost", 5000))
requestSocket.listen(1)
requests = []
#toFinishMap = []
jobRequest = {}

countJobs = 0
countJobsLock = threading.Lock()
jobRequestLock = threading.Lock()


configPath = sys.argv[1]
scheduleMethod = sys.argv[2]
f = open(configPath)
configuration = json.loads(f.read())
print(configuration)

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
			requests.append(jobRequest)
			jobRequestLock.release()
		print(jobRequest)
		print(countJobs)


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
				if(len(j['map_tasks'])):
					chosenTask = j['map_tasks'][0]
					#toFinishMap.append(j['map_tasks'][0])
					j['map_tasks'] = j['map_tasks'][1:]
					findTask=True
					break
					# mapper left so have to do mapper
				'''
					chosenTask = j['reduce_tasks'][0]
					j['reduce_tasks'] = j['reduce_tasks'][1:]
					findTask=True
					if(len(j['reduce_tasks'])==0):
						rOver = True
					break
					#schedule reducers

			if rOver:
				requests = requests[1:]
				'''
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


thread1 = threading.Thread(target = acceptRequest)

thread2 = threading.Thread(target = scanSchedule)
thread1.start()
thread2.start()
thread1.join()
thread2.join()