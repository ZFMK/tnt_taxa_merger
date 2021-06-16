import pyodbc
import warnings
import re

import pudb

import logging
import logging.config

logger = logging.getLogger('gbif_tnt_taxamerger')


class MSSQLConnector():
	def __init__(self, connectionstring = None, config = None, autocommit=False):
		if config is not None:
			self.connectionstring = '''DSN={0};UID={1};PWD={2};Database={3}'''.format(config['DSN'], config['user'], config['passwd'], config['db'])
			self.databasename = config['db']
		elif connectionstring is not None:
			self.connectionstring = connectionstring
			# read the database name from connectionstring and assign it to attribute self.databasename
			matchobj = re.search('Database\=([^;]+)', connectionstring, re.I)
			if matchobj is not None:
				self.databasename = matchobj.groups()[0]
		else:
			raise Exception ('No data base connection parameters given')
		
		'''
		take over attributes from MSSQLConnector class
		'''
		self.con = None
		self.cur = None
		self.open_connection(autocommit)


	def open_connection(self, autocommit=False):
		self.con = self.__mssql_connect()
		self.cur = self.con.cursor()
		if autocommit:
			self.con.autocommit = True


	def __mssql_connect(self):
		try:
			con = pyodbc.connect(self.connectionstring)
		except pyodbc.Error as e:
			logger.critical("Error {0}: {1}".format(*e.args))
			raise
		return con

	def closeConnection(self):
		self.con.close()

	'''
	expose the connection and cursor
	'''
	
	def getCursor(self):
		return self.cur
	
	def getConnection(self):
		return self.con




