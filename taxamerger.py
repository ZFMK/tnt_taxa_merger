#!/usr/bin/env python
# -*- coding: utf-8 -*-



import pudb
import datetime

from GlobalConfig import ConfigReader
from TaxaDB import TaxaDB

from lib.TaxonomySources import TaxonomySources

from lib.TaxaTransfer.TNTTaxaGetter import TNTTaxaGetter
from lib.TaxaTransfer.TNTSynonymsGetter import TNTSynonymsGetter
from lib.TaxaTransfer.TNTCommonNamesGetter import TNTCommonNamesGetter
from lib.TaxaTransfer.TNTRedListGetter import TNTRedListGetter

from lib.TaxaTransfer.GBIFTaxaGetter import GBIFTaxaGetter
from lib.TaxaTransfer.GBIFSynonymsGetter import GBIFSynonymsGetter

from lib.TaxaTransfer.InsertTaxa import InsertTaxa
from lib.TaxaTransfer.InsertSynonyms import InsertSynonyms
from lib.TaxaTransfer.InsertCommonNames import InsertCommonNames
from lib.TaxaTransfer.InsertRedLists import InsertRedLists

from lib.TaxaTransfer.RankRenamer import RankRenamer
from lib.TaxaTransfer.TaxaClosureTable import TaxaClosureTable

from lib.TaxaTransfer.TaxaMerger import TaxaMerger
from lib.TaxaTransfer.SynonymsMerger import SynonymsMerger
from lib.TaxaTransfer.RedListMerger import RedListMerger

import pudb


from configparser import ConfigParser
config = ConfigParser()
config.read('config.ini')


import logging, logging.config


if __name__ == "__main__":
	logging.config.fileConfig('config.ini', defaults={'logfilename': 'gbif_tnt_taxamerger.log'}, disable_existing_loggers=False)
	logger = logging.getLogger('gbif_tnt_taxamerger')
	
	logger.info("\n\n======= S t a r t - {:%Y-%m-%d %H:%M:%S} ======".format(datetime.datetime.now()))
	
	globalconfig = ConfigReader(config)
	taxadb_name = 'TaxaMergerDB'
	
	tnt_sources = globalconfig.tnt_sources
	tstable = TaxonomySources(globalconfig)
	
	taxadb = TaxaDB(globalconfig)
	taxadb_name = taxadb.getDBName()
	for tnt_source in tnt_sources:
		taxsourcename = tnt_source['name']
		tstable.addTaxonomySource(taxsourcename)
		taxsourceid = tstable.getTaxonomySourceID(taxsourcename)
		for projectid in tnt_source['projectids']:
			logger.info("Transfer Taxa from TNT database {0}, project id {1}".format(tnt_source['dbname'], projectid))
			tnttaxagetter = TNTTaxaGetter(tnt_source, tnt_source['dbname'], taxsourceid, projectid)
			inserttaxa = InsertTaxa(tnttaxagetter, globalconfig)
			inserttaxa.copyTaxa()
			
			tntsynonymsgetter = TNTSynonymsGetter(tnt_source, tnt_source['dbname'], taxsourceid, projectid)
			insertsynonyms = InsertSynonyms(tntsynonymsgetter, globalconfig)
			insertsynonyms.copyTaxa()
			
			commonnamesgetter = TNTCommonNamesGetter(tnt_source, tnt_source['dbname'], taxsourceid, projectid)
			insertcommonnames = InsertCommonNames(commonnamesgetter, globalconfig)
			insertcommonnames.copyCommonNames()
			
			tntredlistgetter = TNTRedListGetter(tnt_source, tnt_source['dbname'], taxsourceid, projectid)
			insertredlist = InsertRedLists(tntredlistgetter, globalconfig)
			insertredlist.copyRedList()
	
	
	if globalconfig.use_gbif_taxa is True:
		logger.info("Transfer Taxa from GBIF database {0}".format(globalconfig.gbif_db))
		taxsourcename = globalconfig.gbif_db
		tstable.addTaxonomySource(taxsourcename)
		taxsourceid = tstable.getTaxonomySourceID(taxsourcename)
		gbiftaxagetter = GBIFTaxaGetter(globalconfig.gbif_db, globalconfig.gbif_taxa_table, taxsourceid, globalconfig)
		
		inserttaxa = InsertTaxa(gbiftaxagetter, globalconfig)
		inserttaxa.copyTaxa()
		
		gbifsynonymsgetter = GBIFSynonymsGetter(globalconfig.gbif_db, globalconfig.gbif_taxa_table, taxsourceid, globalconfig)
		insertsynonyms =InsertSynonyms(gbifsynonymsgetter, globalconfig)
		insertsynonyms.copyTaxa()
	
	rankrenamer = RankRenamer(globalconfig)
	
	# the closure table is needed to get and compare the path length to taxa in the different taxonomies
	# therefore, it is created here, but not updated after the taxonomies have been merged
	closuretable = TaxaClosureTable(globalconfig)
	
	# initialize the synonymsmerger before taxa are merged to set the familyCache in synonyms according to closuretable
	synonymsmerger = SynonymsMerger(globalconfig)
	synonymsmerger.setFamilyCache()
	
	redlistmerger = RedListMerger(globalconfig)
	redlistmerger.setFamilyCache()
	
	taxamerger = TaxaMerger(globalconfig)
	
	synonymsmerger.updateAcceptedTaxaIDs()
	redlistmerger.updateTaxaIDs()
	
	
	
	# recreate the closuretable with the current data in TaxaMergeTable
	# this is only needed when the closure table is used in other applications. 
	# sync_dwb_2portal uses only the parent_id from TaxaMergeTable to assign the taxonomic relation, so it is not used there
	#closuretable.createClosureTable()
	#closuretable.fillClosureTable()
	
	
	
	

	logger.info("\n======= E N D - {:%Y-%m-%d %H:%M:%S} ======\n\n".format(datetime.datetime.now()))


