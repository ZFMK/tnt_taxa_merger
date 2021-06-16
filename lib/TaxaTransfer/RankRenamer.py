
import logging
import logging.config

logger = logging.getLogger('gbif_tnt_taxamerger')
log_queries = logging.getLogger('query')


import pudb


from ..MySQLConnector import MySQLConnector


class RankRenamer():
	def __init__(self, globalconfig):
		self.config = globalconfig
		dbconfig = self.config.getTaxaMergerDBConfig()
		self.dbcon = MySQLConnector(dbconfig)
		self.con = self.dbcon.getConnection()
		self.cur = self.dbcon.getCursor()
		
		self.mergetable = "TaxaMergeTable"
		self.rankcodetable = "TaxonomicRanksEnum"
		
		self.pagesize = 10000
		self.setMaxPage()
		
		self.renameRanksAndCodes()


	def renameRanksAndCodes(self):
		page = 1
		while page <= self.maxpage:
			self.renameRanksPage(page)
			self.setRankCodesPage(page)
			page += 1
	
	def renameRanksPage(self, page):
		if page % 10 == 0:
			logger.info("TaxaMerger rename ranks page {0}".format(page))
		startid = ((page-1)*self.pagesize)+1
		lastid = startid + self.pagesize-1
		
		query = """
		UPDATE `{0}` 
		set `rank` = 'sp.',
		rank_code = 170
		WHERE `rank` = 'species'
		AND `id` BETWEEN %s AND %s
		;
		""".format(self.mergetable)
		self.cur.execute(query, [startid, lastid])
		self.con.commit()
		
		query = """
		UPDATE `{0}` 
		set `rank` = 'var.',
		rank_code = 80
		WHERE `rank` = 'variety'
		AND `id` BETWEEN %s AND %s
		;
		""".format(self.mergetable)
		self.cur.execute(query, [startid, lastid])
		self.con.commit()
		
		query = """
		UPDATE `{0}` 
		set `rank` = 'subsp.',
		rank_code = 160
		WHERE `rank` = 'subspecies'
		AND `id` BETWEEN %s AND %s
		;
		""".format(self.mergetable)
		self.cur.execute(query, [startid, lastid])
		self.con.commit()
		
		query = """
		UPDATE `{0}` 
		set `rank` = 'phyl./div.',
		rank_code = 460
		WHERE `rank` = 'phylum'
		AND `id` BETWEEN %s AND %s
		;
		""".format(self.mergetable)
		self.cur.execute(query, [startid, lastid])
		self.con.commit()
		
		query = """
		UPDATE `{0}` 
		set rank_code = 330
		WHERE `rank` = 'subfam.'
		AND `id` BETWEEN %s AND %s
		;
		""".format(self.mergetable)
		self.cur.execute(query, [startid, lastid])
		self.con.commit()
		
		query = """
		UPDATE `{0}` 
		set `rank` = 'fam.',
		rank_code = 340
		WHERE `rank` = 'family' OR `rank` = 'fam.'
		AND `id` BETWEEN %s AND %s
		;
		""".format(self.mergetable)
		self.cur.execute(query, [startid, lastid])
		self.con.commit()
		
		query = """
		UPDATE `{0}` 
		set `rank` = 'ord.',
		rank_code = 380
		WHERE `rank` = 'order'
		AND `id` BETWEEN %s AND %s
		;
		""".format(self.mergetable)
		self.cur.execute(query, [startid, lastid])
		self.con.commit()
		
		query = """
		UPDATE `{0}` 
		set `rank` = 'cl.',
		rank_code = 420
		WHERE `rank` = 'class'
		AND `id` BETWEEN %s AND %s
		;
		""".format(self.mergetable)
		self.cur.execute(query, [startid, lastid])
		self.con.commit()
		
		query = """
		UPDATE `{0}` 
		set `rank` = 'f.',
		rank_code = 60
		WHERE `rank` = 'form'
		AND `id` BETWEEN %s AND %s
		;
		""".format(self.mergetable)
		self.cur.execute(query, [startid, lastid])
		self.con.commit()
		
		query = """
		UPDATE `{0}` 
		set `rank` = 'gen.',
		rank_code = 270
		WHERE `rank` = 'genus'
		AND `id` BETWEEN %s AND %s
		;
		""".format(self.mergetable)
		self.cur.execute(query, [startid, lastid])
		self.con.commit()
		
		query = """
		UPDATE `{0}` 
		set `rank` = 'reg.',
		rank_code = 500
		WHERE `rank` = 'kingdom'
		AND `id` BETWEEN %s AND %s
		;
		""".format(self.mergetable)
		self.cur.execute(query, [startid, lastid])
		self.con.commit()
		
		
		query = """
		UPDATE `{0}` 
		set `rank` = 'infraord.',
		rank_code = 360
		WHERE `rank` = 'infraorder'
		AND `id` BETWEEN %s AND %s
		;
		""".format(self.mergetable)
		self.cur.execute(query, [startid, lastid])
		self.con.commit()
		
	
	
	def setRankCodesPage(self, page):
		if page % 10 == 0:
			logger.info("TaxaMerger set rank codes page {0}".format(page))
		startid = ((page-1)*self.pagesize)+1
		lastid = startid + self.pagesize-1
		
		query = """
		UPDATE `{0}` mt
		INNER JOIN `{1}` rc
		ON (mt.`rank` = rc.`rank`)
		set mt.`rank_code` = rc.`rank_code`
		WHERE mt.`id` BETWEEN %s AND %s
		;
		""".format(self.mergetable, self.rankcodetable)
		self.cur.execute(query, [startid, lastid])
		self.con.commit()
	
	
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
	
