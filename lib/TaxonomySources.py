
import pudb
import re
import pymysql  # -- for MySQl Errors


import logging, logging.config
logger = logging.getLogger('gbif_tnt_taxamerger')
log_missing_taxa = logging.getLogger('missing_taxa')


from .MySQLConnector import MySQLConnector



class TaxonomySources():
	def __init__(self, globalconfig):
		self.config = globalconfig
		dbconfig = self.config.getTaxaMergerDBConfig()
		self.taxamergerdb = MySQLConnector(dbconfig)
		self.con = self.taxamergerdb.getConnection()
		self.cur = self.taxamergerdb.getCursor()
		
	
	def addTaxonomySource(self, taxonomysourcename):
		query = """INSERT INTO `TaxonomySources` (`taxonomy_source_name`)
			VALUES (%s);"""
		self.cur.execute(query, [taxonomysourcename,])
		self.con.commit()
	
	
	def getTaxonomySourceID(self, taxonomysourcename):
		query = """SELECT `TaxonomySourceID` FROM `TaxonomySources`
		WHERE `taxonomy_source_name` = %s
		;"""
		
		self.cur.execute(query, [taxonomysourcename,])
		row = self.cur.fetchone()
		if row is not None:
			return row[0]
		else:
			return None
		
	
	
