from .queue import GlobalQ
from jobs.job import JobStruct
import random
jobID= 0

def makeJobsTestFunc():
	global jobID
	for i in range(10):
		aRand = random.randint(1,100)
		bRand = random.randint(1,100)
		job = JobStruct(jobID,"ADD",[aRand,bRand])
		print(f"Created Job: {job.ID}")
		GlobalQ.put(job)
		print(f"Queued Job: {job.ID}")
		jobID+=1
		print("---")
	