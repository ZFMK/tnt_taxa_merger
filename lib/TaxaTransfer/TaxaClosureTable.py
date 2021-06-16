
import logging
import logging.config

logger = logging.getLogger('gbif_tnt_taxamerger')
log_queries = logging.getLogger('query')


import pudb


from ..MySQLConnector import MySQLConnector


class TaxaClosureTable():
	def __init__(self, globalconfig):
		self.config = globalconfig
		dbconfig = self.config.getTaxaMergerDBConfig()
		self.dbcon = MySQLConnector(dbconfig)
		self.con = self.dbcon.getConnection()
		self.cur = self.dbcon.getCursor()
		
		self.closuretable = "TaxaMergeRelationTable"
		self.taxamergetable = "TaxaMergeTable"
		
		self.createClosureTable()
		self.fillClosureTable()
	
	
	def createClosureTable(self):
		query = """DROP TABLE IF EXISTS `{0}`;""".format(self.closuretable)
		
		self.cur.execute(query)
		self.con.commit()
		
		query = """CREATE TABLE `{0}` (
		`id` int(10) NOT NULL AUTO_INCREMENT,
		`AncestorID` INT(10) NOT NULL, -- SourceTaxonID calculated from SourceTaxonID and SourceParentTaxonID relations
		`DescendantID` INT(10) NOT NULL, -- SourceTaxonID calculated from SourceTaxonID and SourceParentTaxonID relations
		`PathLength` INT(10),
		`TaxonomySourceID` int(10) NOT NULL,
		`SourceProjectID` int(10) DEFAULT NULL,
		PRIMARY KEY (`id`),
		KEY (`AncestorID`),
		KEY (`DescendantID`),
		KEY (`PathLength`),
		KEY (`TaxonomySourceID`),
		KEY (`SourceProjectID`)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
		;""".format(self.closuretable)
		
		self.cur.execute(query)
		self.con.commit()
	
	def getNextLevelCount(self, pathlength):
		query = """
		SELECT COUNT(*)
		FROM `{0}` tr
		WHERE tr.pathLength = %s
		""".format(self.closuretable)
		
		self.cur.execute(query, [pathlength])
		row = self.cur.fetchone()
		if row is not None:
			count = row[0]
		else:
			count = 0
		return count
	
	
	def fillClosureTable(self):
		logger.info("TaxaMerger fill closure table")
		
		#pudb.set_trace()
		# set the relation to it self for each taxon entry
		query = """
		INSERT INTO `{0}` (`id`, `AncestorID`, `DescendantID`, `PathLength`, `TaxonomySourceID`, `SourceProjectID`)
		SELECT NULL, `SourceTaxonID`, `SourceTaxonID`, 0, `TaxonomySourceID`, `SourceProjectID` FROM `{1}`;
		""".format(self.closuretable, self.taxamergetable)
		self.cur.execute(query)
		self.con.commit()
		
		
		pathlength = 0
		levelcount = self.getNextLevelCount(pathlength)
		
		
		while levelcount > 0:
			#pudb.set_trace()
			# set the parent relations
			logger.info("TaxaMerger fill closure table with pathlength {0}, with {1} possible childs".format(pathlength, levelcount))
			query = """
			INSERT INTO `{0}` (`id`, `AncestorID`, `DescendantID`, `PathLength`, `TaxonomySourceID`, `SourceProjectID`)
			SELECT NULL, t2.`SourceTaxonID`, tr.`DescendantID`, tr.pathLength + 1, tr.`TaxonomySourceID`, tr.`SourceProjectID`
			FROM `{0}` tr
			INNER JOIN `{1}` t1 
			ON (
				t1.`SourceTaxonID` = tr.`AncestorID`
				AND t1.`TaxonomySourceID` = tr.`TaxonomySourceID`
				AND t1.`SourceProjectID` = tr.`SourceProjectID`
			)
			INNER JOIN `{1}` t2
			ON(
				t2.`SourceTaxonID` = t1.`SourceParentTaxonID`
				AND t2.`TaxonomySourceID` = t1.`TaxonomySourceID`
				AND t2.`SourceProjectID` = t1.`SourceProjectID`
			)
			WHERE t1.SourceTaxonID != t1.SourceParentTaxonID
			AND tr.pathLength = %s
			""".format(self.closuretable, self.taxamergetable)
			self.cur.execute(query, [pathlength])
			self.con.commit()
			
			pathlength += 1
			levelcount = self.getNextLevelCount(pathlength)
	
	
	
	
		
