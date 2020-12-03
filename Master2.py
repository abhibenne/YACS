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



def sendToWorker(chosenTask,workerNumber):
	if(workerNumber == 0):
		conn, addr = workerSocket1.accept()
	if(workerNumber == 1):
		conn, addr = workerSocket2.accept()
	if(workerNumber == 2):
		conn, addr = workerSocket3.accept()

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
				if(len(j['map_tasks'])):
					chosenTask = j['map_tasks'][0]
					#toFinishMap.append(j['map_tasks'][0])
					if j['job_id'] not in finishRequests.keys():
						finishRequests[j['job_id']]=[]
					finishRequests[j['job_id']].append(chosenTask)
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
#thread3 = threading.Thread(target = reciveUpdates)
thread1.start()
thread2.start()
thread1.join()
thread2.join()