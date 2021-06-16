

import logging
import logging.config

logger = logging.getLogger('gbif_tnt_taxamerger')

import pudb


from .GBIFTaxaGetter import GBIFTaxaGetter


class GBIFSynonymsGetter(GBIFTaxaGetter):
	def __init__(self, gbifdb, taxontable, taxonomysourceid, globalconfig):
		GBIFTaxaGetter.__init__(self, gbifdb, taxontable, taxonomysourceid, globalconfig)
		
		logger.info("Transfer Synonyms from gbif taxonomy")

	
	def getPageQuery(self):
		query = """SELECT {0} AS `TaxonomySourceID`, `SourceTaxonID`, 0 AS `SourceProjectID`, `SourceAcceptedTaxonID`, `canonicalName`, `scientificNameAuthorShip`, `taxonRank`
		FROM `{1}` WHERE `SourceAcceptedTaxonID` IS NOT NULL AND `rownumber` BETWEEN %s AND %s""".format(self.taxsourceid, self.temptable)
		return query
	
	

	def createGBIFTempTable(self):
		# just a temporary table to have the rownumbers for paging
		# is it worth it?
		
		query = """CREATE TEMPORARY TABLE `{0}` 
		(rownumber INT(10) NOT NULL AUTO_INCREMENT,
		`SourceTaxonID` INT(11) NOT NULL,
		`SourceAcceptedTaxonID` INT(11) DEFAULT NULL,
		canonicalName VARCHAR(255),
		scientificNameAuthorShip VARCHAR(255),
		taxonRank VARCHAR(255),
		PRIMARY KEY (`rownumber`)
		) CHARSET=utf8mb4
		;""".format(self.temptable)
		
		self.cur.execute(query)
		self.con.commit()
		
		
		query = """
		INSERT INTO `{0}`
		SELECT DISTINCT NULL, t.TaxonID AS SourceTaxonID, t.acceptedNameUsageID AS SourceAcceptedTaxonID, t.canonicalName, t.scientificNameAuthorShip, t.taxonRank
		FROM `{1}`.`{2}` t WHERE t.taxonomicStatus = 'synonym'
		;
		""".format(self.temptable, self.gbifdb, self.taxontable)
		
		self.cur.execute(query)
		self.con.commit()
		return

