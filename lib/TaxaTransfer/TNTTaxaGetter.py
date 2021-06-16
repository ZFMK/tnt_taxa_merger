

import logging
import logging.config

logger = logging.getLogger('gbif_tnt_taxamerger')

import pudb


from ..MSSQLConnector import MSSQLConnector
from .TaxaGetter import TaxaGetter


class TNTTaxaGetter(TaxaGetter):
	def __init__(self, tntsource, tnt_dbname, tntsourceid, tnt_projectid):
		
		self.tntsource = tntsource
		self.tntsourceid = tntsourceid
		self.tntcon = MSSQLConnector(self.tntsource['connection'])
		TaxaGetter.__init__(self, self.tntcon)
		
		self.tnt_dbname = tnt_dbname
		self.tnt_projectid = tnt_projectid
		self.temptable = '#TNTTempTable'
		
		self.createTNTTempTable()
		self.setMaxPage()
		logger.info("Transfer Taxa from TNT Source {0} {1} {2}".format(tntsource['name'], tnt_dbname, tnt_projectid))
	
	
	def getCountQuery(self):
		query = """
		SELECT COUNT(*) FROM [dbo].[{0}]
		;
		""".format(self.temptable)
		return query
	
	def getPageQuery(self):
		query = """SELECT {0} AS [TaxonomySourceID], [SourceTaxonID], [SourceParentTaxonID], {1} AS [SourceProjectID], [canonicalName], [scientificNameAuthorShip], [parentCanonicalName], [taxonRank], [scientificName]
		FROM [{2}] WHERE [rownumber] BETWEEN ? AND ?""".format(self.tntsourceid, self.tnt_projectid, self.temptable)
		return query
	

	def createTNTTempTable(self):
		#self.dropTNTTempTable()
		
		# Björn merken: SELECT INTO Hashtable funktioniert nie mit Platzhaltern, da MSSQL die Abfrage mit Platzhaltern in einem anderen Scope durchführt!
		
		query = """
		SELECT DISTINCT
		IDENTITY (INT) as rownumber, -- set an IDENTITY column that can be used for paging
		tn1.NameID AS [SourceTaxonID],
		th1.NameParentID AS [SourceParentTaxonID],
		CASE WHEN e1.DisplayOrder < 180 THEN
				RTRIM( -- this fixes cases where InfraspecificEpithet = '' instead of NULL or when it ends with whitespace
				SUBSTRING(tn1.GenusOrSupragenericName,1,1) + LOWER(SUBSTRING(tn1.GenusOrSupragenericName,2,LEN(tn1.GenusOrSupragenericName)-1))
					+ CASE WHEN tn1.SpeciesEpithet IS NULL THEN '' ELSE ' ' + tn1.SpeciesEpithet END
					+ CASE WHEN tn1.InfraspecificEpithet IS NULL THEN '' ELSE ' ' + tn1.InfraspecificEpithet END
				)
			ELSE
				SUBSTRING(tn1.GenusOrSupragenericName,1,1) + LOWER(SUBSTRING(tn1.GenusOrSupragenericName,2,LEN(tn1.GenusOrSupragenericName)-1))
		END AS [canonicalName],
		CASE WHEN e1.DisplayOrder <180 THEN
				case when tn1.NomenclaturalCode = 3 /* Zoology */
				then
					case when  tn1.BasionymAuthors is null or tn1.BasionymAuthors = ''
					then ''
					else
						case when tn1.IsRecombination = 1 then '(' else '' end +
						RTRIM(tn1.BasionymAuthors) +
						case when tn1.IsRecombination = 1 and NOT tn1.BasionymAuthorsYear IS null
							then ', ' + cast(tn1.BasionymAuthorsYear AS varchar)
							else case when tn1.IsRecombination = 0 and not tn1.YearOfPubl is null
								then ', ' + cast(tn1.YearOfPubl AS varchar)
								else '' end
						end
						+ case when tn1.IsRecombination = 1 then ')' else '' end
					end +
					case when tn1.NonNomenclaturalNameSuffix IS NULL then '' else ' ' + RTRIM(tn1.NonNomenclaturalNameSuffix) end
				else
					case when  tn1.BasionymAuthors is null or tn1.BasionymAuthors = ''
					then ''
					else 	case when tn1.CombiningAuthors is null or tn1.CombiningAuthors = ''
						then  	' ' + RTRIM(tn1.BasionymAuthors) +
							case when  tn1.SanctioningAuthor is null or tn1.SanctioningAuthor = ''  then '' else ' : ' + RTRIM(tn1.SanctioningAuthor) end
						else 	' (' + RTRIM(tn1.BasionymAuthors) +
							case when  tn1.SanctioningAuthor is null or tn1.SanctioningAuthor = ''  then '' else ' : ' + RTRIM(tn1.SanctioningAuthor) end
							+ ') '
						end
					end +
					case when  tn1.CombiningAuthors is null or tn1.CombiningAuthors = ''  then '' else RTRIM(tn1.CombiningAuthors) end +
					case when tn1.InfraspecificEpithet is null  or tn1.InfraspecificEpithet = ''
					then ''
					else
						case when tn1.SpeciesEpithet = tn1.InfraspecificEpithet and not tn1.InfraspecificEpithet is null and tn1.InfraspecificEpithet <> ''
						then  ' ' +
							case when tn1.TaxonomicRank is null or tn1.TaxonomicRank = ''
							then ''
							else case when tn1.NomenclaturalCode = 3 /* Zoology */ and  (tn1.TaxonomicRank = 'ssp.' or tn1.TaxonomicRank = 'subsp.') then '' else tn1.TaxonomicRank + ' ' end
							end
							+ RTRIM(tn1.InfraspecificEpithet)
						else ''
						end
					end +
					case when tn1.NonNomenclaturalNameSuffix IS NULL then '' else ' ' + RTRIM(tn1.NonNomenclaturalNameSuffix) end
				end
			ELSE NULL 
		END AS [scientificNameAuthorShip],
		CASE WHEN e2.DisplayOrder < 180 THEN
				RTRIM( -- this fixes cases where InfraspecificEpithet = '' instead of NULL or when it ends with whitespace
				SUBSTRING(tn2.GenusOrSupragenericName,1,1) + LOWER(SUBSTRING(tn2.GenusOrSupragenericName,2,LEN(tn2.GenusOrSupragenericName)-1))
					+ CASE WHEN tn2.SpeciesEpithet IS NULL THEN '' ELSE ' ' + tn2.SpeciesEpithet END
					+ CASE WHEN tn2.InfraspecificEpithet IS NULL THEN '' ELSE ' ' + tn2.InfraspecificEpithet END
				)
			ELSE
				SUBSTRING(tn2.GenusOrSupragenericName,1,1) + LOWER(SUBSTRING(tn2.GenusOrSupragenericName,2,LEN(tn2.GenusOrSupragenericName)-1))
		END AS [parentCanonicalName],
		tn1.TaxonomicRank AS [taxonRank],
		tn1.TaxonNameCache AS [scientificName]
		INTO [{1}]
		FROM DiversityTaxonNames_{0}.dbo.TaxonName AS tn1
		INNER JOIN DiversityTaxonNames_{0}.dbo.TaxonNameProject p ON p.NameID = tn1.NameID 
		LEFT JOIN DiversityTaxonNames_{0}.dbo.TaxonNameList l ON l.NameID=tn1.NameID
		LEFT JOIN DiversityTaxonNames_{0}.dbo.TaxonHierarchy AS th1 ON tn1.NameID = th1.NameID AND th1.ProjectID = p.ProjectID
		LEFT JOIN DiversityTaxonNames_{0}.dbo.TaxonName AS tn2 ON th1.NameParentID = tn2.NameID AND th1.ProjectID = p.ProjectID
		LEFT JOIN DiversityTaxonNames_{0}.dbo.TaxonNameTaxonomicRank_Enum e1 ON e1.Code=tn1.TaxonomicRank
		LEFT JOIN DiversityTaxonNames_{0}.dbo.TaxonNameTaxonomicRank_Enum e2 ON e2.Code=tn2.TaxonomicRank
		LEFT JOIN DiversityTaxonNames_{0}.dbo.TaxonAcceptedName tna ON (
			tna.NameID = tn1.NameID
			and tna.ProjectID = p.ProjectID
			and tna.IgnoreButKeepForReference = 0)
		WHERE p.ProjectID = {2}
		AND tna.NameID IS NOT NULL -- accepted names
		 -- AND tna.NameID IS NULL -- synonyms etc.
		;
		""".format(self.tnt_dbname, self.temptable, self.tnt_projectid)
		
		self.cur.execute(query)
		self.con.commit()
		return


	def dropTempTable(self):
		query = """DROP TABLE [dbo].[{0}];""".format(self.tnt_temptablename)
		self.cur.execute(query)
		self.con.commit()
		return
		
		
