import logging
import logging.config

logger = logging.getLogger('gbif_tnt_taxamerger')

import pudb


from .InsertTaxa import InsertTaxa


class InsertCommonNames(InsertTaxa):
	def __init__(self, taxagetter, globalconfig):
		
		InsertTaxa.__init__(self, taxagetter, globalconfig)
	
	def copyCommonNames(self):
		self.taxa = self.taxagetter.getNextTaxaPage()
		
		while self.taxa is not None:
			self.setPlaceholderString(self.taxa)
			self.setValuesFromLists(self.taxa)
			
			query = """
			INSERT INTO `TaxaCommonNamesTempTable`
			(`TaxonomySourceID`, `SourceTaxonID`, `SourceProjectID`, `name`, `code`, `db_name`)
			VALUES {0}
			;""".format(self.placeholderstring)
			
			self.cur.execute(query, self.values)
			self.con.commit()
			
			self.taxa = self.taxagetter.getNextTaxaPage()
			
	

	
	



