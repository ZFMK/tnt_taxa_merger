#!/usr/bin/env python
# -*- coding: utf8 -*-

import pudb

from lib.MySQLConnector import MySQLConnector


class TaxaDB():
	def __init__(self, globalconfig):
		# connect to MySQL target database and get a connection and cursor object
		dbconfig = globalconfig.getTaxaMergerDBConfig()
		self.taxadb_name = globalconfig.getTaxaMergerDBName()
		
		self.taxadb = MySQLConnector(dbconfig)
		self.cur = self.taxadb.getCursor()
		self.con = self.taxadb.getConnection()
		
		self.createTaxaDBTables()


	def getDBName(self):
		return self.taxadb_name


	def createTaxaDBTables(self):
		self.create_queries = CreateTableQueries()
		for (query) in self.create_queries.getQueriesList():
			self.cur.execute(query)
			self.con.commit()


class CreateTableQueries():
	def __init__(self):
		pass


	def getQueriesList(self):
		queries = []
		queries.extend(self.TaxonomySources())
		queries.extend(self.TaxonomySourcesInsertRootTaxonomy())
		queries.extend(self.TaxaMergeTable())
		queries.extend(self.TaxaMergeTableInsertRootTaxa())
		queries.extend(self.TaxaMergeRelationTable())
		
		queries.extend(self.TaxaSynonymsMergeTable())
		
		queries.extend(self.TaxonomicRanksEnum())
		queries.extend(self.insertTaxonomicRanksEnum())
		
		queries.extend(self.TaxaCommonNamesTempTable())
		
		queries.extend(self.TaxaPropertyTerms())
		queries.extend(self.TaxaPropertyTermsFillFixedValues())
		queries.extend(self.TaxaRedListTempTable())
		return queries


	def TaxonomySources(self):
		q = [
		"""DROP TABLE IF EXISTS `TaxonomySources`""",
		"""CREATE TABLE `TaxonomySources` (
			`TaxonomySourceID` INT(10) NOT NULL AUTO_INCREMENT,
			`taxonomy_source_name` varchar(255) NOT NULL,
			PRIMARY KEY (`TaxonomySourceID`),
			UNIQUE KEY (`taxonomy_source_name`)
			) DEFAULT CHARSET=utf8mb4"""]
		return q


	def TaxonomySourcesInsertRootTaxonomy(self):
		return [
		"""
		INSERT INTO `TaxonomySources` (`TaxonomySourceID`, `taxonomy_source_name`)
		VALUES ('0', 'TaxonomySource for root taxa generated from code')
		;
		""", ]


	def TaxaMergeTable(self):
		return [
		"""DROP TABLE IF EXISTS `TaxaMergeTable`""",
		"""CREATE TABLE `TaxaMergeTable` (
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
		`familyCache` VARCHAR(255) DEFAULT NULL,
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
		;"""
		]


	def TaxaMergeTableInsertRootTaxa(self):
		return [
		"""
		INSERT INTO `TaxaMergeTable` (`id`, `parent_id`, `SourceTaxonID`, `TaxonomySourceID`, `SourceParentTaxonID`, `taxon`, `rank`, `parent_taxon`)
		VALUES (1, NULL, 1, 0, NULL, 'root', 'root', NULL),
		(2, 1, 2, 0, 1, 'Animalia', 'reg.', 'root'),
		(3, 1, 3, 0, 1, 'Plantae', 'reg.', 'root'),
		(4, 1, 4, 0, 1, 'Fungi', 'reg.', 'root')
		;
		""", ]


	def TaxaMergeRelationTable(self):
		return [
		"""DROP TABLE IF EXISTS `TaxaMergeRelationTable`""",
		"""CREATE TABLE `TaxaMergeRelationTable` (
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
		;"""]


	def TaxonomicRanksEnum(self):
		return [
		"""DROP TABLE IF EXISTS `TaxonomicRanksEnum`""",
		"""CREATE TABLE `TaxonomicRanksEnum` (
		`rank` varchar(255),
		`rank_code` int(10) NOT NULL,
		UNIQUE KEY `rank` (`rank`),
		PRIMARY KEY (`rank_code`)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
		;"""]


	def insertTaxonomicRanksEnum(self):
		return [
		"""
		INSERT INTO TaxonomicRanksEnum
		VALUES
		("agg.", 191),
		("aggr.", 190),
		("biovar.", 100),
		("cand.", 10),
		("cl.", 420),
		("convar.", 120),
		("cult.", 110),
		("cultivar. group", 130),
		("dom.", 520),
		("f.", 60),
		("f. sp.", 30),
		("fam.", 340),
		("gen.", 270),
		("graft-chimaera", 140),
		("grex", 165),
		("infracl.", 400),
		("infrafam.", 320),
		("infragen.", 250),
		("infraord.", 360),
		("infraphyl./div.", 440),
		("infrareg.", 480),
		("infrasp.", 150),
		("infratrib.", 280),
		("ord.", 380),
		("pathovar.", 90),
		("phyl./div.", 460),
		("reg.", 500),
		("sect.", 240),
		("ser.", 220),
		("sp.", 170),
		("sp. group", 180),
		("subcl.", 410),
		("subfam.", 330),
		("subfm.", 50),
		("subgen.", 260),
		("subord.", 370),
		("subphyl./div.", 450),
		("subreg.", 490),
		("subsect.", 230),
		("subser.", 210),
		("subsp.", 160),
		("subsubfm.", 40),
		("subtrib.", 290),
		("subvar.", 70),
		("supercl.", 430),
		("superfam.", 350),
		("superord.", 390),
		("superphyl./div.", 470),
		("superreg.", 510),
		("supertrib.", 310),
		("tax. infragen.", 200),
		("tax. infrasp.", 20),
		("tax. supragen.", 530),
		("trib.", 300),
		("var.", 80)
		;""", ]
	

	def TaxaSynonymsMergeTable(self):
		return [
		"""DROP TABLE IF EXISTS `TaxaSynonymsMergeTable`""",
		"""CREATE TABLE `TaxaSynonymsMergeTable` (
		`id` int(10) unsigned NOT NULL AUTO_INCREMENT,
		`SourceTaxonID` int(10) NOT NULL,
		`TaxonomySourceID` int(10) NOT NULL,
		`SourceProjectID` int(10) NOT NULL DEFAULT 0,
		`SourceAcceptedTaxonID`  int(10) NOT NULL,
		`taxon_id` int unsigned COMMENT 'the taxon-id of the synonym',
		`syn_taxon_id` int unsigned COMMENT 'the taxon-id of the accepted name in _Taxa table',
		`taxon` varchar(255) NOT NULL,
		`author` varchar(255) DEFAULT NULL,
		`rank` varchar(25) NOT NULL,
		`accepted_taxon` varchar(255) DEFAULT NULL,
		`familyCache`  VARCHAR(255) DEFAULT NULL,
		PRIMARY KEY (`id`),
		UNIQUE KEY `origin` (`SourceTaxonID`, `TaxonomySourceID`, `SourceProjectID`, `SourceAcceptedTaxonID`),
		KEY `taxon_id` (`taxon_id`),
		KEY `syn_taxon_id` (`syn_taxon_id`),
		KEY `taxon` (`taxon`),
		KEY `rank` (`rank`),
		KEY `accepted_taxon` (accepted_taxon),
		KEY `familyCache` (familyCache)
		) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4"""]


	def TaxaCommonNamesTempTable(self):
		return [
		"""DROP TABLE IF EXISTS `TaxaCommonNamesTempTable`""",
		"""CREATE TABLE `TaxaCommonNamesTempTable` (
		`id` int(10) unsigned NOT NULL AUTO_INCREMENT,
		`SourceTaxonID` int(10) NOT NULL,
		`TaxonomySourceID` int(10) NOT NULL,
		`SourceProjectID` int(10) NOT NULL DEFAULT 0,
		`name` varchar(255) NOT NULL,
		`code` varchar(2) NOT NULL DEFAULT 'de',
		`db_name` varchar(50) NOT NULL,
		PRIMARY KEY (`id`),
		KEY `name` (`name`)
		) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT='Common taxon names from tnt.diversityworkbench.de'"""]


	def TaxaPropertyTerms(self):
		return [
		"""DROP TABLE IF EXISTS `TaxaPropertyTerms`""",
		"""CREATE TABLE `TaxaPropertyTerms` (
		`id` int(10) unsigned NOT NULL AUTO_INCREMENT,
		`term` varchar(800) NOT NULL,
		`category` enum('rl_category','rl_reference') NOT NULL,
		`lang` varchar(10) NOT NULL DEFAULT 'de',
		PRIMARY KEY (`id`)
		) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT='Terms for taxon properties'"""]

	def TaxaPropertyTermsFillFixedValues(self):
		#fixed values because of portal software
		#this must be reworked
		return [
		"""INSERT INTO `TaxaPropertyTerms`
		(`id`, `term`, `category`, `lang`)
		 -- fixed values here because portal software uses ids that depend on TNT SQL queries
		 -- must be reworked
		VALUES 
		(27, 'AktuelleBestandssituation', 'rl_category', 'de'),
		(28, 'KurzfristigerBestandstrend', 'rl_category', 'de'),
		(29, 'LangfristigerBestandstrend', 'rl_category', 'de'),
		(30, 'LetzterNachweis', 'rl_category', 'de'),
		(31, 'Neobiota', 'rl_category', 'de'),
		(32, 'Risikofaktoren', 'rl_category', 'de'),
		(33, 'RL-Kategorie', 'rl_category', 'de'),
		(34, 'Sonderfälle', 'rl_category', 'de'),
		(35, 'Verantwortlichkeit', 'rl_category', 'de')
		;
		""", ]


	def TaxaRedListTempTable(self):
		return [
		"""DROP TABLE IF EXISTS `TaxaRedListTempTable`""",
		"""CREATE TABLE `TaxaRedListTempTable` (
		`id` int(10) unsigned NOT NULL AUTO_INCREMENT,
		`taxon_id` int unsigned COMMENT 'the taxon_id of name in _Taxa table',
		`SourceTaxonID` int(10) NOT NULL,
		`TaxonomySourceID` int(10) NOT NULL,
		`SourceProjectID` int(10) NOT NULL DEFAULT 0,
		`value` varchar(50) NOT NULL,
		`term` varchar(50),
		`reference` varchar(800),
		`category_id` int(10),
		`reference_id` int(10),
		`taxon` varchar(255) DEFAULT NULL,
		`familyCache`  VARCHAR(255) DEFAULT NULL,
		PRIMARY KEY (`id`),
		KEY `taxon_id` (`taxon_id`),
		KEY `SourceTaxonID` (`SourceTaxonID`),
		KEY `TaxonomySourceID` (`TaxonomySourceID`),
		KEY `SourceProjectID` (`SourceProjectID`),
		KEY `category_id` (`category_id`),
		KEY `reference_id` (`reference_id`),
		KEY `taxon` (`taxon`),
		KEY `familyCache` (`familyCache`)
		) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT='Red list values from TNT Analysis'"""]





