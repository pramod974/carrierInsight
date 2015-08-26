__author__ = 'Pramod.Kumar'
import sys
import MySQLdb
import pandas as pd
absoluteFile=sys.argv[1]
class matchCarrier:
    def __init__(self):
        self.savepath=""
        self.get_connection()
        self.dataFrames=[]
    def get_connection(self):
        try:
            self.db = MySQLdb.connect("172.16.0.55","root","admin123*","carrier_insight")
            self.dbCursor=self.db.cursor(MySQLdb.cursors.DictCursor)
        except Exception as e:
            print e
            self.db=None
    def matchNmfta(self,scac):
        sql="""SELECT distinct Scac,MCN,CarrierName as CarrierName_NMFTA,Address,City,State,ZipCode,Country,Phone,DOT FROM nmfta where scac ="%s" """%(scac)
        # self.dbCursor.execute(sql)
        # resultsNmfta=self.dbCursor.fetchall()
        dfNmfta=pd.read_sql_query(sql,self.db)
        if len(dfNmfta) == 0 or dfNmfta.loc[0,"DOT"] == None:
            return 0
        else:
            self.dataFrames.append(dfNmfta)
            status=self.matchMCMIS(int(dfNmfta.loc[0,"DOT"]))
            if status:
                parsedFrame=pd.concat(self.dataFrames,axis=1)
                return parsedFrame
            else:
                # dummyMCMIS=pd.DataFrame(index=[0],columns=[u'censusnum', u'Name', u'nameDBA', u'companyrep1', u'companyrep2', u'phystr', u'phycity', u'physt', u'phyzip', u'maistr', u'maicity', u'maist', u'maizip', u'cellnum', u'telnum', u'emailress', u'fleetsize', u'liqgas', u'owntract', u'totpwr', u'tottrucks', u'trmtract', u'trmtruck', u'trptract', u'trptruck', u'usdotrevokedflag'])
                # self.dataFrames.append(dummyMCMIS)
                parsedFrame=pd.concat(self.dataFrames,axis=1)
                return parsedFrame
    def matchHazmat(self,cennum):
        sql="""SELECT distinct cen_num FROM bulk_and_all_class_3_hazmat_car where scac ="%s" """%(cennum)
        self.dbCursor.execute(sql)
        # resultsHazmat=self.dbCursor.fetchall()
        dfHazmat=pd.read_sql_query(sql,self.db)
        if len(dfHazmat) == 0:
            return False
        else:
            self.dataFrames.append(dfHazmat)
            return True
    def matchMCMIS(self,cennum):
        sql="""select censusnum,
            Name,
            nameDBA,
            companyrep1,
            companyrep2,
            phystr,
            phycity,
            physt,
            phyzip,
            maistr,
            maicity,
            maist,
            maizip,
            cellnum,
            telnum,
            emailress,
            fleetsize,
            liqgas,
            owntract,
            totpwr,
            tottrucks,
            trmtract,
            trmtruck,
            trptract,
            trptruck,
            usdotrevokedflag FROM mcmis
                WHERE  censusnum="%s";"""%(cennum)
        dfMCMIS=pd.read_sql_query(sql,self.db)
        dfMCMIS["MCMIS_Flag"]=0
        if len(dfMCMIS) == 0:
            return False
        else:
            self.dataFrames.append(dfMCMIS)
            return True
class parseCarrierToCSV:
    def __init__(self,absoluteFile):
        self.path=absoluteFile
        self.savepath=""
        self.parsedFrames=[]
    def readCsv(self):
        df=pd.read_csv(self.path,sep='~')
        for scac in df["Scac"]:
            matches=matchCarrier()
            result=matches.matchNmfta(scac)
            if type(result) is not int:
                self.parsedFrames.append(result)
        dffinal=pd.concat(self.parsedFrames,ignore_index=True)
        dffinal=dffinal.drop_duplicates()
        dfparsed=pd.merge(df,dffinal,how='left',left_on="Scac",right_on="Scac")
        dfparsed=dfparsed.applymap(lambda x:str(x).strip())
        self.savepath=self.path.replace('.','_parsed.')
        dfparsed.to_csv(self.savepath)

parser=parseCarrierToCSV(absoluteFile)
parser.readCsv()