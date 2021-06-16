

import logging
import logging.config

logger = logging.getLogger('gbif_tnt_taxamerger')

import pudb


from ..MySQLConnector import MySQLConnector
from .TaxaGetter import TaxaGetter


class GBIFTaxaGetter(TaxaGetter):
	def __init__(self, gbifdb, taxontable, taxonomysourceid, globalconfig):
		# set the db connection before calling parent class
		self.config = globalconfig
		
		# gbifdb is in self mysql instance as tempdb, so i use the same connector
		dbconfig = self.config.getTaxaMergerDBConfig()
		self.dbcon = MySQLConnector(dbconfig)
		TaxaGetter.__init__(self, self.dbcon)
		
		self.temptable = 'GBIFTempTable'
		self.gbifdb = gbifdb
		self.taxontable = taxontable
		self.taxsourceid = taxonomysourceid
		
		self.createGBIFTempTable()
		self.setMaxPage()

	
	def getCountQuery(self):
		query = """
		SELECT COUNT(*) FROM `{0}`
		;
		""".format(self.temptable)
		return query
	
	def getPageQuery(self):
		query = """SELECT {0} AS `TaxonomySourceID`, `SourceTaxonID`, `SourceParentTaxonID`, 0 AS `SourceProjectID`, `canonicalName`, `scientificNameAuthorShip`, `parentCanonicalName`, `taxonRank`, `scientificName`
		FROM `{1}` WHERE `rownumber` BETWEEN %s AND %s""".format(self.taxsourceid, self.temptable)
		return query
	
	

	def createGBIFTempTable(self):
		# just a temporary table to have the rownumbers for paging
		# is it worth it?
		
		query = """CREATE TEMPORARY TABLE `{0}` 
		(rownumber INT(10) NOT NULL AUTO_INCREMENT,
		`SourceTaxonID` INT(11) NOT NULL,
		`SourceParentTaxonID` INT(11) DEFAULT NULL,
		canonicalName VARCHAR(255),
		scientificNameAuthorShip VARCHAR(255),
		parentCanonicalName VARCHAR(255),
		taxonRank VARCHAR(255),
		scientificName VARCHAR(255),
		PRIMARY KEY (`rownumber`)
		) CHARSET=utf8mb4
		;""".format(self.temptable)
		
		self.cur.execute(query)
		self.con.commit()
		
		
		query = """
		INSERT INTO `{0}`
		SELECT DISTINCT NULL, t.TaxonID AS SourceTaxonID, t.parentNameUsageID AS SourceParentTaxonID, t.canonicalName, t.scientificNameAuthorShip, t.parentCanonicalName, t.taxonRank, t.scientificName 
		FROM `{1}`.`{2}` t WHERE t.taxonomicStatus = 'accepted' 
		AND (t.canonicalName IS NOT NULL AND t.canonicalName != "") 
		AND (t.parentCanonicalName IS NOT NULL AND t.parentCanonicalName != "")
		AND (t.kingdom IN ("Animalia")) -- restrict to Animalia, currently no other idea to prevent double names from other kingdoms to be used , "Plantae", "Fungi")
		OR t.canonicalName IN ("Animalia") -- restrict to Animalia, currently no other idea to prevent double names from other kingdoms to be used , "Plantae", "Fungi")
		;
		""".format(self.temptable, self.gbifdb, self.taxontable)
		
		self.cur.execute(query)
		self.con.commit()
		return


	def dropTempTable(self):
		query = """DROP TABLE `{0}`;""".format(self.temptable)
		self.cur.execute(query)
		self.con.commit()
		return
		
		
