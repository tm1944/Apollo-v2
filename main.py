from producer.producer import GlobalQ, makeJobsTestFunc
from producer.queue import printQueue
from worker.worker import checkQ, doJob


def main():
	#produce 10 jobs for now
	makeJobsTestFunc()


	while checkQ():
		res,jobID = doJob(GlobalQ.get())
		print(f"Worker completed Job : {jobID} => {res}")

if __name__ == "__main__":
	main()