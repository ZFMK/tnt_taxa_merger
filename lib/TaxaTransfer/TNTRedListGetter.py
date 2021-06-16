

import logging
import logging.config

logger = logging.getLogger('gbif_tnt_taxamerger')
log_query = logging.getLogger('query')

import pudb


from .TNTTaxaGetter import TNTTaxaGetter

class TNTRedListGetter(TNTTaxaGetter):
	def __init__(self, tntsource, tnt_dbname, tntsourceid, tnt_projectid):
		# how to deal with that long lists of ids?
		# are there any red list data that should not be published?
		
		self.projectclauses = {
			'Animalia': "\tAND l.ProjectID BETWEEN 381 AND 881 ",
			'Insecta': "\tAND l.ProjectID BETWEEN 381 AND 881 ",
			'Vertebrata': "\tAND l.ProjectID BETWEEN 923 AND 927 "
		}
		
		try:
			self.projectclause = self.projectclauses[tnt_dbname]
		except KeyError:
			self.projectclause = ""
		
		
		TNTTaxaGetter.__init__(self, tntsource, tnt_dbname, tntsourceid, tnt_projectid)
		
		self.temptable = "#RedListTempTable"
		self.createTNTTempTable()
		self.setMaxPage()
		logger.info("Transfer Red List Properties and References from TNT Source {0} {1} {2}".format(tntsource['name'], tnt_dbname, tnt_projectid))
	
	
	def getCountQuery(self):
		query = """
		SELECT COUNT(*) FROM [{0}]
		;
		""".format(self.temptable)
		return query
	
	def getPageQuery(self):
		query = """SELECT 
		{0} AS [TaxonomySourceID],
		t.[NameID] AS [SourceTaxonID],
		{1} AS [SourceProjectID],
		[Term],
		[Value],
		[Reference]
		FROM [{2}] t
		WHERE t.[rownumber] BETWEEN ? AND ?""".format(self.tntsourceid, self.tnt_projectid, self.temptable)
		return query
	
	
	
	def createTNTTempTable(self):
		"""
		create a table that contains all references and terms in the red list that should be looked up
		in the taxon names databases
		See it as a configuration part, insert the references of red lists and terms you want to have queried from taxon names
		"""
		
		query = """
		SELECT 
		IDENTITY (INT) as rownumber,
		a.NameID, a.Term, a.Value, a.Reference
		INTO [{0}]
		FROM (
		SELECT a.AnalysisID, l.NameID, SUBSTRING(ac.DisplayText,18,255) AS Term, a.AnalysisValue AS Value, lr.TaxonNameListRefText AS Reference
		FROM DiversityTaxonNames_{1}.dbo.TaxonNameList l
			INNER JOIN DiversityTaxonNames_{1}.dbo.TaxonNameListReference lr ON lr.NameID=l.NameID AND lr.ProjectID=l.ProjectID
			LEFT JOIN DiversityTaxonNames_{1}.dbo.TaxonNameListAnalysis a ON a.NameID=lr.NameID AND a.ProjectID=lr.ProjectID
			LEFT JOIN DiversityTaxonNames_{1}.dbo.TaxonNameListAnalysisCategory ac ON ac.AnalysisID=a.AnalysisID
		WHERE
			ac.DisplayText IN (
			 -- Vertebrata
			'RoteListe_D_2009_LangfristigerBestandstrend',
			'RoteListe_D_2009_Verantwortlichkeit',
			'RoteListe_D_2009_AktuelleBestandssituation',
			'RoteListe_D_2009_RL-Kategorie',
			'RoteListe_D_2009_KurzfristigerBestandstrend',
			'RoteListe_D_2009_Sonderfälle',
			'RoteListe_D_2009_Neobiota',
			'RoteListe_D_2009_LetzterNachweis',
			'RoteListe_D_2009_Risikofaktoren',
			 -- Insecta and Animalia
			'RoteListe_D_2016_RL-Kategorie',
			'RoteListe_D_2016_AktuelleBestandssituation',
			'RoteListe_D_2016_LangfristigerBestandstrend',
			'RoteListe_D_2016_KurzfristigerBestandstrend',
			'RoteListe_D_2016_Risikofaktoren',
			'RoteListe_D_2016_Verantwortlichkeit',
			'RoteListe_D_2016_Sonderfälle',
			'RoteListe_D_2016_Neobiota',
			'RoteListe_D_2016_LetzterNachweis',
			'RoteListe_D_2011_AktuelleBestandssituation',
			'RoteListe_D_2011_KurzfristigerBestandstrend',
			'RoteListe_D_2011_LangfristigerBestandstrend',
			'RoteListe_D_2011_Risikofaktoren',
			'RoteListe_D_2011_RL-Kategorie',
			'RoteListe_D_2011_Verantwortlichkeit',
			'RoteListe_D_2011_Neobiota',
			'RoteListe_D_2011_LetzterNachweis'
			) {2}
		) AS a
		;""".format(self.temptable, self.tnt_dbname, self.projectclause)
		
		
		self.cur.execute(query)
		self.con.commit()
		


