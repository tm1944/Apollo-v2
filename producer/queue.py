import queue

GlobalQ = queue.Queue()

def printQueue():
	for job in list(GlobalQ.queue):
		print(job)