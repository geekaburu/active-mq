#--------------------------------------------------------------------------------------
#	Author  : Alvin Kaburu
#	Date    : 5th July 2017
#	Program : 1. Connect to database
#			  2. Read from database
#			  3. Write to CSV file
#			
#			  1. Connect to active mq queue
#			  2. Dequeue the queue
#			  3. Write to CSV file
#--------------------------------------------------------------------------------------


import sys,json,MySQLdb,time,stomp
from modules import readAllFromTable

# class to collect all messages received from the queue
class QueueListener(object):
	messages = []
	def __init__(self):
		self.messages = []

	def on_message(self, headers, msg):
		self.messages.append(msg)

# create csv file from database or queue result
def writeToCSVFile(results,file):
	buffer = []
	for row in results:
		# foreach entry in the row
		for entry in row:
			# append results into a temporary buffer
			buffer.append(entry)
		# convert the buffer to a csv annd write it to a file
		file.write('|'.join(str(entry) for entry in buffer)+'\n')
		buffer = []

#--------------------------------------------------------------------------------------
#	create database connection and fetch results 
#	write the results to a csv file
#--------------------------------------------------------------------------------------

databaseConnection = MySQLdb.connect("localhost","root","", "greenshoe")
print('Created connection to database...')
tableName = 'greenshoe'
print('Connected to database...')
cursor = databaseConnection.cursor()
results = readAllFromTable(cursor,tableName)
print('Fetched results from database...')

# write collected results to csv file
file = open('database.csv','w') 
writeToCSVFile(results,file)
print('Finished writing to file....')

#--------------------------------------------------------------------------------------
#	create active mq connection and fetch results from the queue
#	write the results to a csv file
#--------------------------------------------------------------------------------------

activeConnection = stomp.Connection10() 
print('Created Active MQ connection...')
listener = QueueListener()
activeConnection.set_listener('QueueListener', listener)
activeConnection.start()
activeConnection.connect()
activeConnection.subscribe('greenshoe')
print('Subscribed to connection...')
time.sleep(1)
results = listener.messages

data = []
for result in results:
	# format data to readable format
	data.append(list(json.loads(result).values()))

file = open('activemq.csv','w')
writeToCSVFile(data,file)
print('Finished writing queue data to file...')

#--------------------------------------------------------------------------------------
#	disconnect all connections
#	close open file
#--------------------------------------------------------------------------------------

activeConnection.disconnect()
file.close()
databaseConnection.close()
print('Closed all active connections...')