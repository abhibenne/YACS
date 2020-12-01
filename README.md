# BD_final_assignment_YACS
Big Data final project - YACS


Execution:
-> Execute the files in the following order, on 5 separate terminals.
  1. python3 master.py config.json <scheduling algorithm>            # scheduling algorithm = Random or Round or Least
  2. python3 requests.py <no_of_requests>
  3. python3 worker.py 4000 1
  4. python3 worker.py 4001 2
  5. python3 worker.py 4002 3
  6. python3 Analysis.py <alg>       #alg = Random or Round or Least

Files:
 1. config.json:
    Holds information on the configuration of workers -> worker_id, slots, port
 2. requests.py:
    Generates job requests
 3. master.py:
    Listens to job Requests and schedules the tasks.
 4. worker.py:
    Executes tasks and notifies master on completion of the task.
 5. Analysis.py:
    Plots the graphs(Bar graph and heatmap) to analise the output.

   
