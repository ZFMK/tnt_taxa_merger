#!/usr/bin/env python
# -*- coding: utf8 -*-
from configparser import ConfigParser, NoOptionError
import re
import pudb

"""
This class was only needed for translation of some more advanced configuration settings.
It provides a single object where all other classes can access the configuration parameters.
"""


class ConfigReader():

	def __init__(self, config = None):
		self.config = config
		if not (isinstance(config, ConfigParser)):
			raise ValueError ('ConfigReader.__init__(): parameter config must be instance of ConfigParser')

		self.readStaticParameters()
		self.readTaxaMergerDBParameters()
		self.readTNTSourceParameters()


	def readStaticParameters(self):
		
		if self.config.has_option('option', 'use_gbif_taxa'):
			self.use_gbif_taxa = self.config.getboolean('option', 'use_gbif_taxa')
			if self.use_gbif_taxa is True:
				self.gbif_db = self.config.get('GBIF_DB', 'db')
				self.gbif_taxa_table = self.config.get('GBIF_DB', 'table')
		else:
			self.use_gbif_taxa = False
		
		
	
	def readTaxaMergerDBParameters(self):
		self.taxadb_config = {
			'db': self.config.get('taxamergerdb', 'db'),
			'user': self.config.get('taxamergerdb', 'user'),
			'passwd': self.config.get('taxamergerdb', 'passwd')
			}
		
		try:
			self.taxadb_config['host'] = self.config.get('taxamergerdb', 'host')
		except NoOptionError:
			self.taxadb_config['host'] = 'localhost'
		
		try:
			self.taxadb_config['port'] = self.config.get('taxamergerdb', 'port')
		except NoOptionError:
			self.taxadb_config['port'] = 3306
		
		try:
			self.taxadb_config['charset'] = self.config.get('taxamergerdb', 'charset')
		except NoOptionError:
			self.taxadb_config['charset'] = 'utf8'
		
		self.taxadb_name = self.taxadb_config['db']
	
	def getTaxaMergerDBName(self):
		return self.taxadb_name
		
	def getTaxaMergerDBConfig(self):
		return self.taxadb_config
	

	def readTNTSourceParameters(self):
		'''
		assign all necessary config values depending on a data source
		'''
		self.tnt_sources = []
		sections = self.config.sections()
		for section in sections:
			if section[:4]=='tnt_' and section!='tnt_test':
				tnt_source = {}
				tnt_source['name'] = section
				tnt_source['connection'] = self.config.get(section, 'connection')
				tnt_source['dbname'] = self.config.get(section, 'dbname')
				tnt_source['projectids'] = [projectid.strip() for projectid in self.config.get(section, 'projectids').split(',')]
				self.tnt_sources.append(tnt_source)
	

