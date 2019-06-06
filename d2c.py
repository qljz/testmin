import pyodbc
import sys
import csv
import argparse
import os
#import cx_Oracle

server = 'tcp:10.12.61.11'
database = 'run'
username = 'sa'
password = 'Zjgs6321'
parser = argparse.ArgumentParser(description='d2c')
parser.add_argument('-i', type=str, help='database ip')

def ocdb(args):
	print("read from oracle db...")
	driver = 'DRIVER={' + args.driver + '};SERVER='
	server = 'tcp:' + args.ip
	database = args.database
	username = args.user
	password = args.pswd
	try:
		cnxn = cx_Oracle.connect(username, password, server + ':1521/' + database)
		c = cnxn.cursor()
		table = args.table
		#sql = "select * from " + table + " where status = '4'"
		sql = "select * from " + table 
		print(sql)
		c.execute(sql)
	except Exception as e:
		print(e)
	writecsv(c, args.file)

def cdb(args):
	print("read from db...")
	driver = 'DRIVER={' + args.driver + '};SERVER='
	server = 'tcp:' + args.ip
	database = args.database
	username = args.user
	password = args.pswd
	try:
		cnxn = pyodbc.connect(driver+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
		c = cnxn.cursor()
		c.execute('select @@version')
		r = c.fetchone()
		while r:
			#print(r)
			r = c.fetchone()
		
		table = args.table
		#sql = "select * from " + table + " where status = '4'"
		sql = "select * from " + table 
		print(sql)
		c.execute(sql)
	except Exception as e:
		print(e)
	writecsv(c, args.file)

def writecsv(c, filename):
	print("read to " + filename)
	try:
		f = open(filename, 'w', newline='')
		wtr = csv.writer(f)
		wtr.writerow([x[0] for x in c.description])
		for r in c.fetchall():
			#print(r)
			wtr.writerow(r)
		f.close()
	except Exception as e:
		print(e)

if __name__ == "__main__":
	print("process args...")
	print(sys.argv)
	#arguments
	parser.add_argument('driver', type=str, help='driver')
	parser.add_argument('ip', type=str, help='database ip')
	parser.add_argument('user', type=str, help='user')
	parser.add_argument('pswd', type=str, help='user password')
	parser.add_argument('database', type=str, help='database')
	parser.add_argument('table', type=str, help='table')
	parser.add_argument('file', type=str, help='file')
	parser.add_argument('dbt', nargs='?', default="sqlserver")
	argss = parser.parse_args()
	if argss.dbt == "sqlserver":
		cdb(argss)
	if argss.dbt == "oracle":
		ocdb(argss)
	print("end")
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	