

import logging
import logging.config

logger = logging.getLogger('gbif_tnt_taxamerger')
log_query = logging.getLogger('query')

import pudb


from .TNTTaxaGetter import TNTTaxaGetter

class TNTCommonNamesGetter(TNTTaxaGetter):
	def __init__(self, tntsource, tnt_dbname, tntsourceid, tnt_projectid):
		
		TNTTaxaGetter.__init__(self, tntsource, tnt_dbname, tntsourceid, tnt_projectid)
		
		logger.info("Transfer taxa common names from TNT Source {0} {1} {2}".format(tntsource['name'], tnt_dbname, tnt_projectid))
	
	
	def getCountQuery(self):
		query = """
		SELECT COUNT(*) FROM [dbo].[{0}]
		;
		""".format(self.temptable)
		return query
	
	def getPageQuery(self):
		query = """SELECT 
		{0} AS [TaxonomySourceID],
		t.[NameID] AS [SourceTaxonID],
		{1} AS [SourceProjectID],
		[CommonName],
		[Code],
		[db_name]
		FROM [{2}] t
		WHERE t.[rownumber] BETWEEN ? AND ?""".format(self.tntsourceid, self.tnt_projectid, self.temptable)
		return query
	
	
	
	def createTNTTempTable(self):
		# Björn merken: SELECT INTO Hashtable funktioniert nie mit Platzhaltern, da MSSQL die Abfrage mit Platzhaltern in einem anderen Scope durchführt!
		
		query = """
		SELECT DISTINCT
		IDENTITY (INT) as rownumber,
		NameID, CAST(CommonName AS nvarchar(255)) AS CommonName, db_name, CAST(t.LanguageCode AS varchar(2)) AS Code 
		INTO [{0}]
		FROM
		(SELECT tn.NameID, tc.CommonName, '{1}' AS db_name, tc.LanguageCode FROM DiversityTaxonNames_{1}.dbo.TaxonCommonName tc
			INNER JOIN DiversityTaxonNames_{1}.dbo.TaxonName tn ON tc.NameID=tn.NameID
			LEFT JOIN DiversityTaxonNames_{1}.dbo.TaxonNameProject p ON p.NameID=tc.NameID
		WHERE p.ProjectID = {2}
		) as t
		;""".format(self.temptable, self.tnt_dbname, self.tnt_projectid)
		
		self.cur.execute(query)
		self.con.commit()
		

