from producer.queue import GlobalQ
from jobs.definework import add
from jobs.job import JobStruct

def checkQ()->bool:
	return not GlobalQ.empty()

def doJob(jobStruct: JobStruct):
	print(f"Worker picked up Job: {jobStruct.ID}")
	match jobStruct.Type:
		case "ADD":
			a = jobStruct.Data[0]
			b = jobStruct.Data[1]
			return add(a,b), jobStruct.ID
		case _:
			print("Unregonized Job TYPE")
