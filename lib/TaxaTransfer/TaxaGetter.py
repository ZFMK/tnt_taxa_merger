import logging
import logging.config

logger = logging.getLogger('gbif_tnt_taxamerger')

import pudb




class TaxaGetter():
	def __init__(self, dbconnector):
		self.dbcon = dbconnector
		
		self.con = self.dbcon.getConnection()
		self.cur = self.dbcon.getCursor()
		
		self.pagesize = 10000
		self.page = 1
		self.taxanum = 0
		self.maxpage = 0
		self.pagingstarted = False
	
	
	def setTaxaNum(self):
		query = self.getCountQuery()
		self.cur.execute(query)
		row = self.cur.fetchone()
		if row is not None:
			self.taxanum = row[0]
		else:
			self.taxanum = 0
	
	def getTaxaNum(self):
		self.setTaxaNum()
		return self.taxanum
	
	def setMaxPage(self):
		taxanum = self.getTaxaNum()
		if taxanum > 0:
			self.maxpage = int(taxanum / self.pagesize + 1)
		else:
			self.maxpage = 0
	
	def initPaging(self):
		self.currentpage = 1
		self.pagingstarted = True
	
	def getNextTaxaPage(self):
		if self.pagingstarted == False:
			self.initPaging()
		if self.currentpage <= self.maxpage:
			taxa = self.getTaxaPage(self.currentpage)
			self.currentpage = self.currentpage + 1
			#return taxa
			if len(taxa) > 0:
				return taxa
			else:
				# this should never happen as self.maxpage should be 0 when there are no results
				#pudb.set_trace()
				# this can happen with names that are not accepted names and not synonyms and therefore are not be found by pageQuery in TNTSynonymsGetter 
				self.pagingstarted = False
				return None
		else:
			self.pagingstarted = False
			return None
	
	
	def getTaxaPage(self, page):
		if page % 10 == 0:
			logger.info("TaxaGetter get taxa page {0}".format(page))
		startrow = ((page-1)*self.pagesize)+1
		lastrow = startrow + self.pagesize-1
		
		parameters = [startrow, lastrow]
		query = self.getPageQuery()
		
		self.cur.execute(query, parameters)
		taxa = self.cur.fetchall()
		return taxa
	
