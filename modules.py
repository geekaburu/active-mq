import time, json

# convert string to dictionary
def strToJson(keys,values):
	return json.dumps(dict(zip(keys,values)))

# read the csv file
def readCSV(file):
	with open(file, 'r') as csv:
		# replace pipes with null
		return csv.read().replace('|','')

# create table
def createTable(cursor,name,keys):
	# Drop table if it already exists
	cursor.execute("DROP TABLE IF EXISTS {}".format(name.upper()))
	# Create table to store the values
	cursor.execute('CREATE TABLE {} ({});'.format(name.upper(), ','.join("`"+key+"`"+" CHAR(4) NOT NULL" for key in keys)))
	return

# insert CSV values to database 
def sendCSVToDatabase(cursor,table,strBuffer):
	cursor.execute('INSERT INTO {} VALUES ({});'.format(table, ','.join("'" + item + "'" for item in strBuffer)))

# return all values from a table
def readAllFromTable(cursor,table):
	cursor.execute('SELECT * FROM {};'.format(table))
	return cursor.fetchall()

# insert CSV values to Active MQ
def sendToActiveMQ(connection,destination,jsonObject,txid):
	connection.send(body=jsonObject, destination=destination,transaction=txid)
	# time.sleep(0)
	return