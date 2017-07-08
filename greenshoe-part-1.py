#--------------------------------------------------------------------------------------
#	Author  : Alvin Kaburu
#	Date    : 5th July 2017
#	Program : 1. Connect to database
#			  2. Read data from CSV
#			  3. Filter data to be written to database
#			  3. Write data to database
#			
#			  1. Connect to active mq queue
#			  3. Read data from CSV file
#			  3. Filter data to be queued to Active MQ
#			  4. Queue data to Active MQ
#--------------------------------------------------------------------------------------


import sys,MySQLdb,stomp
from modules import strToJson,readCSV,createTable,sendCSVToDatabase,sendToActiveMQ

# Create program variables
i = 0
databaseState = 0
cursor = 0
strBuffer = ''
databaseKeys = ['0','1','2','3','4','5','6','7','8','9','10','11']
fileName = sys.argv[1]

#--------------------------------------------------------------------------------------
#	create database connection and create table
#--------------------------------------------------------------------------------------

databaseConnection = MySQLdb.connect("localhost","root","", "greenshoe")
print('Connected to your database...')
tableName = 'greenshoe'
cursor = databaseConnection.cursor()
createTable(cursor, tableName, databaseKeys)
print('Created table...')

#--------------------------------------------------------------------------------------
#	create Active MQ connection and subscribe to destination
#--------------------------------------------------------------------------------------
activeConnection = stomp.Connection([('127.0.0.1', 61613)])
print('Connected to your Active MQ...')
activeConnection.start()
activeConnection.connect('admin', 'admin', wait=True)
destination = '/queue/greenshoe'
activeConnection.subscribe(destination=destination, id=1, ack='auto')
print('Subscribed to queue...')
transaction = activeConnection.begin()


#--------------------------------------------------------------------------------------
#	process each csv row and send to database or commit transactions
#--------------------------------------------------------------------------------------
for char in readCSV(fileName):
	#if char is not newline
	if char !='\n':
		# Check the 5th index for C or G
		if i == 4 and (char == 'C' or char == 'G'):
			#Activate database state
			databaseState = 1
		# Add characters into a buffer
		strBuffer+=char
	else:
		# if database state is active, call database function
		if databaseState:
			sendCSVToDatabase(cursor,tableName,strBuffer)
			print('Sent {} to database....'.format(strBuffer))
			databaseState = 0
		# else add to Actieve MQ
		else:
			sendToActiveMQ(activeConnection,destination,strToJson(databaseKeys,strBuffer),transaction)
			print('Queued {} to ActiveMQ...'.format(strBuffer))
		strBuffer = ''

	#Reset i value
	if i == 13:
		i = 0
	i+=1

# Commit active mq connection
activeConnection.commit(transaction)


#--------------------------------------------------------------------------------------
#	close all connections
#--------------------------------------------------------------------------------------
activeConnection.disconnect()
databaseConnection.close()
print('Closed all connections...')