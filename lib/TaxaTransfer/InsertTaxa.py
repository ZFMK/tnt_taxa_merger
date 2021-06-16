import logging
import logging.config

logger = logging.getLogger('gbif_tnt_taxamerger')

import pudb


from ..MySQLConnector import MySQLConnector


class InsertTaxa():
	def __init__(self, taxagetter, globalconfig):
		self.taxagetter = taxagetter
		self.config = globalconfig
		
		dbconfig = self.config.getTaxaMergerDBConfig()
		self.dbcon = MySQLConnector(dbconfig)
		
		self.con = self.dbcon.getConnection()
		self.cur = self.dbcon.getCursor()
		
		self.taxa = []
	
	
	
	
	def copyTaxa(self):
		self.taxa = self.taxagetter.getNextTaxaPage()
		
		while self.taxa is not None:
			self.setPlaceholderString(self.taxa)
			self.setValuesFromLists(self.taxa)
			
			query = """
			INSERT INTO `TaxaMergeTable`
			(`TaxonomySourceID`, `SourceTaxonID`, `SourceParentTaxonID`, `SourceProjectID`, `taxon`, `author`, `parent_taxon`, `rank`, `scientificName`)
			VALUES {0}
			;""".format(self.placeholderstring)
			
			self.cur.execute(query, self.values)
			self.con.commit()
			
			self.taxa = self.taxagetter.getNextTaxaPage()
		
		
		
	def setValuesFromLists(self, dataslice):
		self.values = []
		for valuelist in dataslice:
			self.values.extend(valuelist)
		
		
		
	def setPlaceholderStrings(self, dataslice):
		"""
		extra method to get a list(!) of placeholder strings to be able to combine them with fixed values like NULL in the VALUES lists of the INSERT query
		"""
		self.placeholderstrings = []
		for valuelist in dataslice:
			placeholders = ['%s'] * len(valuelist)
			self.placeholderstrings.append(', '.join(placeholders))
	
	def setPlaceholderString(self, dataslice):
		self.setPlaceholderStrings(dataslice)
		self.placeholderstring = '(' + '), ('.join(self.placeholderstrings) + ')'
	



