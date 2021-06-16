
import logging
import logging.config

logger = logging.getLogger('gbif_tnt_taxamerger')
log_queries = logging.getLogger('query')


import pudb


from ..MySQLConnector import MySQLConnector


class TaxaMerger():
	def __init__(self, globalconfig):
		self.config = globalconfig
		dbconfig = self.config.getTaxaMergerDBConfig()
		self.dbcon = MySQLConnector(dbconfig)
		self.con = self.dbcon.getConnection()
		self.cur = self.dbcon.getCursor()
		
		self.mergetable = "TaxaMergeTable"
		self.taxamergeclosuretable = "TaxaMergeRelationTable"
		
		self.targettable = "Taxa"
		
		self.familytemptable = "tofamily"
		self.genustemptable = "togenus"
		self.subspeciestemptable = "tosubspecies"
		
		self.pagesize = 10000
		self.setMaxPage()
		
		self.createToFamilyTable()
		self.createFamilyToGenusTable()
		self.createGenusToSubSpeciesTable()
		
		self.markDoublesToFamily()
		self.markDoublesToGenus()
		self.markDoublesToSubSpecies()
		
		self.updateDoubleFlagInMergeTable()
		self.updateFamilyCacheInMergeTable()
		self.deleteDoubles()
		#pudb.set_trace()
		self.updateParentIDs()
		self.moveTaxaWithSameRanks()
		self.deleteUnconnectedTaxa()


	def markDoublesByPathLength(self, tablename):
		query = """
		SELECT MAX(pathLength) FROM `{0}`
		;
		""".format(self.taxamergeclosuretable)
		self.cur.execute(query)
		row = self.cur.fetchone()
		if row is None:
			maxpathlength = 0
		else:
			maxpathlength = int(row[0])
		
		for pathlength in range(maxpathlength, 0, -1):
			
			logger.info("TaxaMerger mark double names with max pathlength {0}".format(pathlength))
			# iterate through the pathlengthes with max pathlength first to select the name with the longest path
			# if there are two names with the same max pathlength, chose the one with the lowest id
			query = """
			 -- set name doubles to 4 for the row with the lowest id of the same taxon
			 -- thus the doubled rows that where inserted earlier where taken later for merging the child taxa of other taxonomy sources
			UPDATE `{0}` t1
			INNER JOIN `{1}` tr ON (
				t1.SourceTaxonID = tr.DescendantID
				AND t1.TaxonomySourceID = tr.TaxonomySourceID
				AND t1.SourceProjectID = tr.SourceProjectID
			)
			INNER JOIN (
				SELECT MIN(t2.id) as id, t2.taxon FROM `{0}` t2
				 -- there must be an inner join to taxonrelation too to restrict the pathlength in this subselect too, otherwise a min id can be selected that is connected to another pathlength
				INNER JOIN `{1}` tr2 ON (
					t2.SourceTaxonID = tr2.DescendantID 
					AND t2.TaxonomySourceID = tr2.TaxonomySourceID 
					AND t2.SourceProjectID = tr2.SourceProjectID)
				WHERE tr2.pathlength = %s
					AND t2.name_doubles = 2
				GROUP BY taxon
				) as t2
			ON (t1.id = t2.id)
			SET name_doubles = 4
			WHERE tr.pathLength = %s
			AND t1.name_doubles = 2
			;
			""".format(tablename, self.taxamergeclosuretable)
			self.cur.execute(query, [pathlength, pathlength])
			self.con.commit()
			
			
			# set the doubles with shorter path to 6
			query = """
			UPDATE `{0}` t1
			INNER JOIN `{0}` t2 ON (
				 -- t1.scientificName = t2.scientificName
				t1.taxon = t2.taxon
			)
			SET t1.name_doubles = 6
			WHERE t1.name_doubles = 2
			AND t2.name_doubles = 4
			""".format(tablename)
			self.cur.execute(query)
			self.con.commit()
		
		
		# set the taxa with longer path back to 0 again
		query = """UPDATE `{0}` t1
		SET t1.name_doubles = 0
		WHERE t1.name_doubles = 4
		""".format(tablename)
		self.cur.execute(query)
		self.con.commit()
		return


	def setMaxPage(self):
		query = """
		SELECT MAX(id) FROM `{0}`
		;
		""".format(self.mergetable)
		self.cur.execute(query)
		row = self.cur.fetchone()
		if row[0] is not None:
			taxanum = row[0]
			self.maxpage = int(taxanum / self.pagesize + 1)
		else:
			taxanum = 0
			self.maxpage = 0
		return


	def updateParentIDs(self):
		#pudb.set_trace()
		page = 1
		while page <= self.maxpage:
			self.updateParentIDsPage(page)
			page += 1
		return
	
	
	def updateParentIDsPage(self, page):
		startid = ((page-1)*self.pagesize)+1
		lastid = startid + self.pagesize-1
		
		#if startid < 73640 and lastid > 73640:
		#	pudb.set_trace()
		
		if page % 10 == 0:
			logger.info("TaxaMerger update parent ids page {0} of {1} pages".format(page, self.maxpage))
		
		query = """
		 -- update the parent_id with the parent_taxon given in merge table
		 -- use the trees existing in tnt sources
		UPDATE `{0}` mt1
		INNER JOIN `{0}` mt2 ON (mt1.SourceParentTaxonID = mt2.SourceTaxonID 
			AND mt1.TaxonomySourceID = mt2.TaxonomySourceID
			AND mt1.SourceProjectID = mt2.SourceProjectID)
		SET mt1.parent_id = mt2.`id`
		WHERE mt1.name_doubles = 0 AND mt2.name_doubles = 0
		AND mt2.rank_code > mt1.rank_code
		AND mt1.id BETWEEN %s AND %s
		;
		""".format(self.mergetable)
		log_queries.info('{0} {1} {2}'.format(query, startid, lastid))
		
		self.cur.execute(query, [startid, lastid])
		self.con.commit()
		
		query = """
		 -- update the parent_id with the parent_taxon given in merge table
		 -- where a parent from another source should be used 
		 -- but the taxon is in the same family
		UPDATE `{0}` mt1
		INNER JOIN `{0}` mt2 ON (mt1.parent_taxon = mt2.taxon)
		SET mt1.parent_id = mt2.`id`
		 -- connect taxa that have no parent taxon or are connected to them self to the selected parent taxon from another source
		WHERE mt1.parent_id IS NULL
		AND mt2.rank_code > mt1.rank_code
		 -- do not allow subgenus here
		 -- AND mt1.taxon != mt2.taxon
		AND mt1.familyCache = mt2.familyCache
		AND mt1.name_doubles = 0
		AND mt2.name_doubles = 0
		AND mt1.id BETWEEN %s AND %s
		;
		""".format(self.mergetable)
		log_queries.info('{0} {1} {2}'.format(query, startid, lastid))
		
		self.cur.execute(query, [startid, lastid])
		self.con.commit()
		
		query = """
		 -- update the parent_id with the parent_taxon given in merge table
		 -- where a parent from another source should be used 
		 -- but the taxon is not in the same family
		UPDATE `{0}` mt1
		INNER JOIN `{0}` mt2 ON (mt1.parent_taxon = mt2.taxon)
		SET mt1.parent_id = mt2.`id`
		 -- connect taxa that have no parent taxon or are connected to them self to the selected parent taxon from another source
		WHERE mt1.parent_id IS NULL
		AND mt2.rank_code > mt1.rank_code
		 -- do not allow subgenus here
		 -- AND mt1.taxon != mt2.taxon
		AND mt1.name_doubles = 0
		AND mt2.name_doubles = 0
		AND mt1.id BETWEEN %s AND %s
		;
		""".format(self.mergetable)
		log_queries.info('{0} {1} {2}'.format(query, startid, lastid))
		
		self.cur.execute(query, [startid, lastid])
		self.con.commit()
		
		query = """
		 -- update the parent_id with the parent_taxon given in merge table
		 -- where a parent from another source should be used 
		 -- but the taxon is not in the same family
		 -- and where taxon must be connected to an taxon on the same rank
		UPDATE `{0}` mt1
		INNER JOIN `{0}` mt2 ON (mt1.parent_taxon = mt2.taxon)
		SET mt1.parent_id = mt2.`id`
		 -- connect taxa that have no parent taxon or are connected to them self to the selected parent taxon from another source
		WHERE mt1.parent_id IS NULL
		AND mt2.rank_code = mt1.rank_code
		 -- do not allow subgenus here
		AND mt1.taxon != mt2.taxon
		AND mt1.name_doubles = 0
		AND mt2.name_doubles = 0
		AND mt1.id BETWEEN %s AND %s
		;
		""".format(self.mergetable)
		log_queries.info('{0} {1} {2}'.format(query, startid, lastid))
		
		self.cur.execute(query, [startid, lastid])
		self.con.commit()
	
	
	def moveTaxaWithSameRanks(self):
		#pudb.set_trace()
		logger.info("TaxaMerger move taxa with the same rank as parent or child taxon")
		
		# do this two times not more
		for i in range(2):
		
			# do not use paging here as it makes the things more complicated?
			query = """
			UPDATE `{0}` mt
			INNER JOIN `{0}` p1 ON (mt.parent_id = p1.id)
			SET mt.name_doubles = 5
			WHERE 
			mt.rank_code = p1.rank_code
			 -- also update the taxa that have been set to name_doubles = 1,3,5 in the round before
			AND mt.name_doubles IN (0, 1, 3, 5)
			;""".format(self.mergetable)
			log_queries.info('{0}'.format(query))
			
			self.cur.execute(query)
			self.con.commit()
			
			
			query = """
			UPDATE `{0}` mt
			INNER JOIN `{0}` c1 ON (mt.id = c1.parent_id)
			SET mt.name_doubles = 1
			WHERE 
			mt.rank_code = c1.rank_code
			 -- also the taxa that have been set to name_doubles = 1,3,5 in the round before
			AND mt.name_doubles IN (0, 1, 3, 5)
			;""".format(self.mergetable)
			log_queries.info('{0}'.format(query))
			
			self.cur.execute(query)
			self.con.commit()
			
			
			# set taxa that have the same rank as their parent and child taxon to name_doubles = 5 so that they are not moved
			
			query = """
			UPDATE `{0}` mt
			INNER JOIN `{0}` p1 ON (mt.parent_id = p1.id AND mt.rank_code = p1.rank_code)
			INNER JOIN `{0}` c1 ON (mt.id = c1.parent_id AND mt.rank_code = c1.rank_code)
			SET mt.name_doubles = 3
			WHERE mt.name_doubles IN (1, 5)
			;""".format(self.mergetable)
			log_queries.info('{0}'.format(query))
			
			self.cur.execute(query)
			self.con.commit()
			
			
			query = """
			 -- update taxon rank for taxa that have been connected to a child taxon with the same rank
			 -- try the next higher rank
			UPDATE `{0}` mt
			INNER JOIN  `{0}` p1 
			ON (mt.parent_id = p1.id)
			INNER JOIN TaxonomicRanksEnum re
			ON (
				re.rank_code = (mt.rank_code + 10)
			)
			SET mt.rank_code = (mt.rank_code + 10),
			mt.`rank` = re.`rank`,
			mt.name_doubles = 0
			WHERE mt.name_doubles = 1
			AND mt.rank_code + 10 < p1.rank_code
			AND mt.rank_code < 490 AND mt.rank_code > 280
			;""".format(self.mergetable)
			log_queries.info('{0}'.format(query))
			
			self.cur.execute(query)
			self.con.commit()
		
		
			query = """
			 -- update taxon rank for taxa that have been connected to a parent taxon with the same rank
			 -- try the next lower rank
			UPDATE `{0}` mt
			INNER JOIN  `{0}` c1 
			ON (mt.id = c1.parent_id)
			INNER JOIN TaxonomicRanksEnum re
			ON (
				re.rank_code = (mt.rank_code - 10)
			)
			SET mt.rank_code = (mt.rank_code - 10),
			mt.`rank` = re.`rank`,
			mt.name_doubles = 0
			WHERE mt.name_doubles = 5
			AND mt.rank_code - 10 > c1.rank_code
			AND mt.rank_code < 490 AND mt.rank_code > 280
			;""".format(self.mergetable)
			log_queries.info('{0}'.format(query))
			
			self.cur.execute(query)
			self.con.commit()
			
			# reset the information about taxa with same ranks to have a clean start in the next round
			query = """
			UPDATE `{0}` mt
			SET mt.name_doubles = 0
			WHERE mt.name_doubles IN (1, 3, 5)
			;""".format(self.mergetable)
			log_queries.info('{0}'.format(query))
			
			self.cur.execute(query)
			self.con.commit()
		
		
		return
	


	def updateFamilyCacheInMergeTable(self):
		page = 1
		while page <= self.maxpage:
			self.updateFamilyCacheInMergeTablePage(page)
			page += 1
		return


	def updateFamilyCacheInMergeTablePage(self, page):
		if page % 10 == 0:
			logger.info("TaxaMerger update familyCache page {0}".format(page))
		startid = ((page-1)*self.pagesize)+1
		lastid = startid + self.pagesize-1
		
		
		for tablename in [self.familytemptable, self.genustemptable, self.subspeciestemptable]:
			query = """UPDATE `{0}` mt
			INNER JOIN `{1}` tt ON (mt.id = tt.id)
			set mt.familyCache = tt.familyCache
			WHERE tt.name_doubles = 0
			AND tt.id BETWEEN %s AND %s
			""" .format(self.mergetable, tablename)
			self.cur.execute(query, [startid, lastid])
			self.con.commit()
		
		return


	def deleteDoubles(self):
		page = 1
		while page <= self.maxpage:
			self.deleteDoublesPage(page)
			page += 1


	def deleteDoublesPage(self, page):
		# delete all doubles because they have NULL as parent_id
		if page % 10 == 0:
			logger.info("TaxaMerger delete doubled taxa page {0}".format(page))
		startid = ((page-1)*self.pagesize)+1
		lastid = startid + self.pagesize-1
		
		deletequery = """
		DELETE FROM `{0}` WHERE name_doubles != 0
		AND `id` BETWEEN %s AND %s
		;""".format(self.mergetable)
		self.cur.execute(deletequery, [startid, lastid])
		self.con.commit()


	def updateDoubleFlagInMergeTable(self):
		query = """UPDATE `{0}` mt
		set mt.name_doubles = 8
		WHERE taxon != 'root'
		""" .format(self.mergetable)
		self.cur.execute(query)
		self.con.commit()
		
		for tablename in [self.familytemptable, self.genustemptable, self.subspeciestemptable]:
			page = 1
			while page <= self.maxpage:
				self.updateDoubleFlagInMergeTablePage(page, tablename)
				page += 1
		
		return


	def updateDoubleFlagInMergeTablePage(self, page, tablename):
		if page % 10 == 0:
			logger.info("TaxaMerger update name_doubles in Merge Table with name_doubles from table {0}, page {1}".format(tablename, page))
		startid = ((page-1)*self.pagesize)+1
		lastid = startid + self.pagesize-1
		
		
		query = """UPDATE `{0}` mt
		INNER JOIN `{1}` tt ON (mt.id = tt.id)
		set mt.name_doubles = 0
		WHERE tt.name_doubles = 0
		AND tt.id BETWEEN %s AND %s
		""" .format(self.mergetable, tablename)
		self.cur.execute(query, [startid, lastid])
		self.con.commit()


	def createToFamilyTable(self):
		whereclause = """`rank_code` BETWEEN 340 AND 500 """
		self.createTaxaTempTable(self.familytemptable, whereclause)
		
		return


	def createFamilyToGenusTable(self):
		whereclause = """`rank_code` BETWEEN 270 AND 330 """
		self.createTaxaTempTable(self.genustemptable, whereclause)
		return


	def createGenusToSubSpeciesTable(self):
		whereclause = """`rank_code` BETWEEN 150 AND 260 """
		self.createTaxaTempTable(self.subspeciestemptable, whereclause)
		return


	def markDoublesToFamily(self):
		self.prepareTempTable(self.familytemptable)
		logger.info("TaxaMerger markDoublesToFamily")
		
		query = """
		 -- mark all rows that have the same taxon by setting name_doubles to 2, irrespectively if they have the same parent taxon or not
		 -- this must be done first, because setting a clause  t1.parent_taxon != t2.parent_taxon will not find results where one of the parent_taxon is NULL
		UPDATE `{0}` t1 
		INNER JOIN 
		`{0}` t2 ON(
		 -- t1.scientificName = t2.scientificName
		t1.taxon = t2.taxon
		AND t1.id != t2.id)
		set t1.name_doubles = 2
		;
		""".format(self.familytemptable)
		self.cur.execute(query)
		self.con.commit()
		
		self.markDoublesByPathLength(self.familytemptable)
		
		logger.info("TaxaMerger markDoublesToFamily done")
		return


	def markDoublesToGenus(self):
		logger.info("TaxaMerger markDoublesToGenus")
		self.prepareTempTable(self.genustemptable)
		
		self.setFamilyCache(self.genustemptable)
		
		query = """
		 -- mark all rows that have the same taxon by setting name_doubles to 2, irrespectively if they have the same parent taxon or not
		 -- this must be done first, because setting a clause  t1.parent_taxon != t2.parent_taxon will not find results where one of the parent_taxon is NULL
		UPDATE `{0}` t1 
		INNER JOIN 
		`{0}` t2 ON(
		 -- t1.scientificName = t2.scientificName
		t1.taxon = t2.taxon 
		AND t1.id != t2.id
		AND t1.familyCache = t2.familyCache)
		set t1.name_doubles = 2
		;
		""".format(self.genustemptable)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		 -- set name_doubles with no familyCache to 6 to prevent that they are put elsewhere in the tree
		UPDATE `{0}` t1 
		INNER JOIN 
		`{0}` t2 ON(
			t1.taxon = t2.taxon
			AND t1.id != t2.id
		)
		set t1.name_doubles = 6
		WHERE t1.familyCache IS NULL
		""".format(self.genustemptable)
		self.cur.execute(query)
		self.con.commit()
		
		self.markDoublesByPathLength(self.genustemptable)
		
		logger.info("TaxaMerger markDoublesToGenus done")
		return


	def markDoublesToSubSpecies(self):
		logger.info("TaxaMerger markDoublesToSubSpecies")
		self.prepareTempTable(self.subspeciestemptable)
		
		self.setFamilyCache(self.subspeciestemptable)
		
		logger.info("TaxaMerger markDoublesToSubSpecies mark doubles with same familyCache")
		query = """
		 -- mark all rows that have the same taxon by setting name_doubles to 2, irrespectively if they have the same parent taxon or not
		 -- this must be done first, because setting a clause  t1.parent_taxon != t2.parent_taxon will not find results where one of the parent_taxon is NULL
		UPDATE `{0}` t1 
		INNER JOIN 
		`{0}` t2 ON(
		 -- t1.scientificName = t2.scientificName
		t1.taxon = t2.taxon 
		AND t1.id != t2.id
		AND t1.familyCache = t2.familyCache)
		set t1.name_doubles = 2
		;
		""".format(self.subspeciestemptable)
		log_queries.info('{0}'.format(query))
		self.cur.execute(query)
		self.con.commit()
		logger.info("TaxaMerger markDoublesToSubSpecies mark doubles with same familyCache done")
		
		logger.info("TaxaMerger markDoublesToSubSpecies mark doubles with no familyCache")
		query = """
		 -- set name_doubles with no familyCache to 6 to prevent that they are put elsewhere in the tree
		UPDATE `{0}` t1 
		INNER JOIN 
		`{0}` t2 ON(
			t1.taxon = t2.taxon
			AND t1.id != t2.id
		)
		set t1.name_doubles = 6
		WHERE t1.familyCache IS NULL
		""".format(self.subspeciestemptable)
		log_queries.info('{0}'.format(query))
		self.cur.execute(query)
		self.con.commit()
		logger.info("TaxaMerger markDoublesToSubSpecies mark doubles with no familyCache done")
		
		self.markDoublesByPathLength(self.subspeciestemptable)
		
		logger.info("TaxaMerger markDoublesToSubSpecies done")
		return


	def setFamilyCache(self, tablename):
		#pudb.set_trace()
		page = 1
		while page <= self.maxpage:
			self.setFamilyCachePage(page, tablename)
			page += 1
		#pudb.set_trace()
		logger.info("TaxaMerger set family cache table {2} done".format(page, self.maxpage, tablename))
		return


	def setFamilyCachePage(self, page, tablename):
		#pudb.set_trace()
		startid = ((page-1)*self.pagesize)+1
		lastid = startid + self.pagesize-1
		
		if page % 10 == 0:
			logger.info("TaxaMerger set family cache page {0} of {1} pages in table {2}".format(page, self.maxpage, tablename))
		
		query = """
		UPDATE `{0}` to1
		INNER JOIN (
		SELECT
		t1.*, t2.taxon as family FROM `{0}` t1
		INNER JOIN `{1}` tr ON (
		tr.DescendantID = t1.SourceTaxonID
		AND tr.TaxonomySourceID = t1.TaxonomySourceID
		AND tr.SourceProjectID = t1.sourceProjectID)
		INNER JOIN `{2}` t2
		ON(
		tr.AncestorID = t2.SourceTaxonID
		AND tr.TaxonomySourceID = t2.TaxonomySourceID
		AND tr.SourceProjectID = t2.sourceProjectID)
		WHERE t2.`rank` = 'fam.'
		) as to2
		ON (to1.SourceTaxonID = to2.SourceTaxonID
		AND to1.TaxonomySourceID = to2.TaxonomySourceID
		AND to1.SourceProjectID = to2.sourceProjectID)
		SET to1.familyCache = to2.family
		WHERE to1.id BETWEEN %s AND %s
		;""".format(tablename, self.taxamergeclosuretable, self.mergetable)
		log_queries.info('{0} {1} {2}'.format(query, startid, lastid))
		
		self.cur.execute(query, [startid, lastid])
		self.con.commit()
		
		return


	def createTaxaTempTable(self, tablename, whereclause):
		query = """
		DROP TABLE IF EXISTS `{0}`
		;""".format(tablename)
		
		self.cur.execute(query)
		self.con.commit()
		
		query = """CREATE TABLE `{0}` (
		`id` int(10) NOT NULL AUTO_INCREMENT,
		`SourceTaxonID` int(10) NOT NULL,
		`TaxonomySourceID` int(10) NOT NULL,
		`SourceParentTaxonID` int(10) DEFAULT NULL,
		`SourceProjectID` int(10) NOT NULL DEFAULT 0,
		`taxon` varchar(255) NOT NULL,
		`author` varchar(255) DEFAULT NULL,
		`parent_taxon` varchar(255),
		`rank` varchar(25) NOT NULL,
		`parent_id` int(10) unsigned DEFAULT NULL,
		`rank_code` int(10) NOT NULL DEFAULT 0,
		`scientificName` varchar(255),
		`matched_in_specimens` BOOLEAN DEFAULT 0,
		`name_doubles` TINYINT(1) DEFAULT 0, -- 1 as marker for doubles with same parent taxon, 2 as marker for doubles with different parent taxon
		`familyCache` varchar(255),
		PRIMARY KEY (`id`),
		UNIQUE KEY `origin` (`SourceTaxonID`, `TaxonomySourceID`, `SourceParentTaxonID`, `SourceProjectID`),
		KEY `TaxonomySourceID` (`TaxonomySourceID`),
		KEY `SourceParentTaxonID` (`SourceParentTaxonID`),
		KEY `SourceProjectID` (`SourceProjectID`),
		KEY `parent_id` (`parent_id`),
		KEY `taxon` (`taxon`),
		KEY `parent_taxon` (`parent_taxon`),
		KEY `rank` (`rank`),
		KEY `idx_rank_code` (`rank_code` ASC),
		KEY `scientificName` (`scientificName`),
		KEY `matched_in_specimens` (`matched_in_specimens`),
		KEY `name_doubles` (name_doubles),
		KEY `familyCache` (familyCache)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
		;""".format(tablename)
		
		self.cur.execute(query)
		self.con.commit()
		
		query = """INSERT INTO `{0}`
		SELECT 
		`id`,
		`SourceTaxonID`,
		`TaxonomySourceID`,
		`SourceParentTaxonID`,
		`SourceProjectID`,
		`taxon`,
		`author`,
		`parent_taxon`,
		`rank`,
		`parent_id`,
		`rank_code`,
		`scientificName`,
		`matched_in_specimens`,
		`name_doubles`,
		NULL
		FROM `{1}`
		WHERE {2}
		;""".format(tablename, self.mergetable, whereclause)
		
		self.cur.execute(query)
		self.con.commit()
		
		# debugging
		'''
		query = """
		SELECT * FROM {0}
		WHERE taxon = 'Zingel' OR taxon = 'Rutilus pigus'
		""".format(tablename)
		self.cur.execute(query)
		rows = self.cur.fetchall()
		if len(rows) > 0:
			pudb.set_trace()
		'''


	def prepareTempTable(self, tablename):
		query = """UPDATE `{0}` t1
		set t1.name_doubles = 0
		""" .format(tablename)
		self.cur.execute(query)
		self.con.commit()
		
		query = """DELETE tt FROM `{0}` tt
		WHERE tt.parent_taxon IS NULL
		;""" .format(tablename)
		self.cur.execute(query)
		self.con.commit()
		return


	def deleteUnconnectedTaxa(self):
		# this appears to be much faster when ids and parent_ids can be used instead of taxon and parent_taxon, so it is called after the parent_ids are set by self.updateParentIDs()
		logger.info("TaxaMerger delete taxa with no parent taxon")
		#pudb.set_trace()
		deletequery = """
		 -- delete all taxa that have no parent taxon except the root 
		DELETE FROM `{0}` where parent_id IS NULL AND taxon NOT IN ('root');
		""".format(self.mergetable)
		self.cur.execute(deletequery)
		self.con.commit()
		
		selectquery = """
		SELECT COUNT(*) FROM `{0}` t1 LEFT JOIN `{0}` t2 ON(t1.parent_id = t2.`id`) 
		WHERE t2.`id` IS NULL AND t1.taxon NOT IN ('root');
		""".format(self.mergetable)
		
		deletequery = """
		 -- delete all taxa that are connected to deleted taxa 
		DELETE t1 FROM `{0}` t1 LEFT JOIN `{0}` t2 ON(t1.parent_id = t2.`id`) 
		WHERE t2.`id` IS NULL AND t1.taxon NOT IN ('root');
		""".format(self.mergetable)
		
		self.cur.execute(selectquery)
		row = self.cur.fetchone()
		while row[0] > 0:
			#pudb.set_trace()
			self.cur.execute(deletequery)
			self.con.commit()
			self.cur.execute(selectquery)
			row = self.cur.fetchone()
	

