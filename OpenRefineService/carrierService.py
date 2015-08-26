__author__ = 'pramod.kumar'
__author__ = 'Pramod.Kumar'
from flask import Flask,jsonify,abort,request
import json
import MySQLdb
import urlparse
app = Flask(__name__)

tasks = [{}]
@app.route('/')
def hello_world():
    return 'Hello World !'
@app.route('/tasks',  methods=['POST', 'GET'])
def get_tasks():
    query = request.form.get('query')
    return jsonify({'name':'Percentage Allocation Reconsiliation service','percentageAlloction': query})
    # return jsonify({'name':'Percentage Allocation Reconsiliation service','percentageAlloction': query})

@app.route('/task/<carrier>', methods=['POST','GET'])
def get_task(carrier):
    try:
        carrierKey =  str(carrier).replace('+',' ').strip()
        db = MySQLdb.connect("172.16.0.55","root","admin123*","rack_analysis")
        dbCursor=db.cursor(MySQLdb.cursors.DictCursor)
        # sql="SELECT distinct supplier,account_type,terminal_name,product_name FROM rack_analysis.supplier_details_contract where supplier='%s' and Account_Type='MANSFIELD OIL CO OF GAINESVILL-10003000UB' and terminal_name='HARTFORD IL PSX CLLC - 0394' and product_name='B11';"%(carrierKey)
        # sql="SELECT distinct name FROM rack_analysis.mcmis1 where name like '%"+carrierKey+"%'"
        sql="""SELECT distinct name,namedba,CENSUSNUM FROM rack_analysis.mcmis1 where name ="%s" or namedba ="%s" """%(carrierKey,carrierKey)
        dbCursor.execute(sql)
        resultsMcmis=dbCursor.fetchall()
        sql="""SELECT distinct `carrier_name`,`DOT`,`scac` FROM pdedb.nmfta where `carrier_name` ="%s" """%(carrierKey)
        dbCursor.execute(sql)
        resultsNmfta=dbCursor.fetchall()

        if carrierKey == None:
            db.close()
            return jsonify({})
        db.close()
        finalResult={'MCMIS':resultsMcmis,'NMFTA': resultsNmfta}
        # print finalResult
        return jsonify(finalResult)

    except Exception as e:
        return jsonify({'exception':e})

@app.route('/mcmisname/<carrier>', methods=['POST','GET'])
def get_mcmisName(carrier):
    try:
        carrierKey =  str(carrier).replace('+',' ').strip()
        db = MySQLdb.connect("172.16.0.55","root","admin123*","rack_analysis")
        dbCursor=db.cursor(MySQLdb.cursors.DictCursor)
        # sql="SELECT distinct supplier,account_type,terminal_name,product_name FROM rack_analysis.supplier_details_contract where supplier='%s' and Account_Type='MANSFIELD OIL CO OF GAINESVILL-10003000UB' and terminal_name='HARTFORD IL PSX CLLC - 0394' and product_name='B11';"%(carrierKey)
        # sql="SELECT distinct name FROM rack_analysis.mcmis1 where name like '%"+carrierKey+"%'"
        sql="""SELECT distinct name,namedba,CENSUSNUM FROM rack_analysis.mcmis1 where name ="%s" or namedba ="%s" """%(carrierKey,carrierKey)
        dbCursor.execute(sql)
        resultsMcmis=dbCursor.fetchall()
        if carrierKey == None:
            db.close()
            return jsonify({})
        db.close()
        finalResult={'MCMIS':resultsMcmis}
        # print finalResult
        return jsonify(finalResult)

    except Exception as e:
        return jsonify({'exception':e})

@app.route('/mcmisnamelike/<carrier>', methods=['POST','GET'])
def get_mcmisNameLike(carrier):
    try:
        carrierKey =  str(carrier).replace('+',' ').strip()
        # carrierKey="%"+carrierKey+"%"
        db = MySQLdb.connect("172.16.0.55","root","admin123*","rack_analysis")
        dbCursor=db.cursor(MySQLdb.cursors.DictCursor)
        # sql="SELECT distinct supplier,account_type,terminal_name,product_name FROM rack_analysis.supplier_details_contract where supplier='%s' and Account_Type='MANSFIELD OIL CO OF GAINESVILL-10003000UB' and terminal_name='HARTFORD IL PSX CLLC - 0394' and product_name='B11';"%(carrierKey)
        # sql="SELECT distinct name FROM rack_analysis.mcmis1 where name like '%"+carrierKey+"%'"
        # sql="""SELECT distinct name,namedba,CENSUSNUM FROM rack_analysis.mcmis1 where name like "%s" or namedba like "%s" """%(carrierKey,carrierKey)
        sql="""SELECT distinct `name`,`CENSUSNUM`,`namedba` FROM rack_analysis.mcmis1
        WHERE  MATCH (name,namedba)
        AGAINST ('"%s"' IN NATURAL LANGUAGE MODE);"""%(carrierKey)
        dbCursor.execute(sql)
        resultsMcmis=dbCursor.fetchall()
        if carrierKey == None:
            db.close()
            return jsonify({})
        db.close()
        finalResult={'MCMIS':resultsMcmis}
        # print finalResult
        return jsonify(finalResult)

    except Exception as e:
        return jsonify({'exception':e})

@app.route('/nmftaname/<carrier>', methods=['POST','GET'])
def get_nmftaName(carrier):
    try:
        carrierKey =  str(carrier).replace('+',' ').strip()
        db = MySQLdb.connect("172.16.0.55","root","admin123*","rack_analysis")
        dbCursor=db.cursor(MySQLdb.cursors.DictCursor)
        # sql="SELECT distinct supplier,account_type,terminal_name,product_name FROM rack_analysis.supplier_details_contract where supplier='%s' and Account_Type='MANSFIELD OIL CO OF GAINESVILL-10003000UB' and terminal_name='HARTFORD IL PSX CLLC - 0394' and product_name='B11';"%(carrierKey)
        # sql="SELECT distinct name FROM rack_analysis.mcmis1 where name like '%"+carrierKey+"%'"
        sql="""SELECT distinct `carrier_name`,`DOT`,`scac` FROM pdedb.nmfta where `carrier_name` ="%s" """%(carrierKey)
        dbCursor.execute(sql)
        resultsNmfta=dbCursor.fetchall()

        if carrierKey == None:
            db.close()
            return jsonify({})
        db.close()
        finalResult={'NMFTA': resultsNmfta}
        # print finalResult
        return jsonify(finalResult)

    except Exception as e:
        return jsonify({'exception':e})

@app.route('/nmftanamelike/<carrier>', methods=['POST','GET'])
def get_nmftaNameLike(carrier):
    try:
        carrierKey =  str(carrier).replace('+',' ').strip()
        carrierKey="%"+carrierKey+"%"
        db = MySQLdb.connect("172.16.0.55","root","admin123*","rack_analysis")
        dbCursor=db.cursor(MySQLdb.cursors.DictCursor)
        # sql="SELECT distinct supplier,account_type,terminal_name,product_name FROM rack_analysis.supplier_details_contract where supplier='%s' and Account_Type='MANSFIELD OIL CO OF GAINESVILL-10003000UB' and terminal_name='HARTFORD IL PSX CLLC - 0394' and product_name='B11';"%(carrierKey)
        # sql="SELECT distinct name FROM rack_analysis.mcmis1 where name like '%"+carrierKey+"%'"
        sql="""SELECT distinct `carrier_name`,`DOT`,`scac` FROM pdedb.nmfta where `carrier_name` like "%s" """%(carrierKey)
        dbCursor.execute(sql)
        resultsNmfta=dbCursor.fetchall()

        if carrierKey == None:
            db.close()
            return jsonify({})
        db.close()
        finalResult={'NMFTA': resultsNmfta}
        # print finalResult
        return jsonify(finalResult)

    except Exception as e:
        return jsonify({'exception':e})

@app.route('/mcmis/<cennum>', methods=['POST','GET'])
def get_mcmis(cennum):
    try:
        carrierKey =  str(cennum).replace('+',' ').strip()
        db = MySQLdb.connect("172.16.0.55","root","admin123*","rack_analysis")
        dbCursor=db.cursor(MySQLdb.cursors.DictCursor)
        # sql="SELECT distinct supplier,account_type,terminal_name,product_name FROM rack_analysis.supplier_details_contract where supplier='%s' and Account_Type='MANSFIELD OIL CO OF GAINESVILL-10003000UB' and terminal_name='HARTFORD IL PSX CLLC - 0394' and product_name='B11';"%(carrierKey)
        # sql="SELECT distinct name FROM rack_analysis.mcmis1 where name like '%"+carrierKey+"%'"
        sql="""SELECT * FROM rack_analysis.mcmis1 where CENSUSNUM ="%s" """%(carrierKey)
        dbCursor.execute(sql)
        resultsMcmis=dbCursor.fetchall()

        if carrierKey == None:
            db.close()
            return jsonify({})
        db.close()
        # print resultsMcmis
        return jsonify({"mcmis":resultsMcmis})

    except Exception as e:
        return jsonify({'exception':e})

@app.route('/mcmis1/<cennum>', methods=['POST','GET'])
def get_mcmisNormal(cennum):
    try:
        carrierKey =  str(cennum).replace('+',' ').strip()
        db = MySQLdb.connect("172.16.0.55","root","admin123*","rack_analysis")
        dbCursor=db.cursor()
        # sql="SELECT distinct supplier,account_type,terminal_name,product_name FROM rack_analysis.supplier_details_contract where supplier='%s' and Account_Type='MANSFIELD OIL CO OF GAINESVILL-10003000UB' and terminal_name='HARTFORD IL PSX CLLC - 0394' and product_name='B11';"%(carrierKey)
        # sql="SELECT distinct name FROM rack_analysis.mcmis1 where name like '%"+carrierKey+"%'"
        sql="""SELECT * FROM rack_analysis.mcmis1 where CENSUSNUM ="%s" """%(carrierKey)
        dbCursor.execute(sql)
        resultsMcmis=dbCursor.fetchall()

        if carrierKey == None:
            db.close()
            return jsonify({})
        db.close()
        # print resultsMcmis
        # return jsonify({"mcmis":resultsMcmis[0]})
        if len(resultsMcmis)>0:
            return json.dumps(resultsMcmis[0])
        else:
            return json.dumps([])
    except Exception as e:
        return jsonify({'exception':e})

@app.route('/nmftascac/<scac>', methods=['POST','GET'])
def get_nmfta(scac):
    try:
        carrierKey =  str(scac).replace('+',' ').strip()
        db = MySQLdb.connect("172.16.0.55","root","admin123*","rack_analysis")
        dbCursor=db.cursor(MySQLdb.cursors.DictCursor)
        # sql="SELECT distinct supplier,account_type,terminal_name,product_name FROM rack_analysis.supplier_details_contract where supplier='%s' and Account_Type='MANSFIELD OIL CO OF GAINESVILL-10003000UB' and terminal_name='HARTFORD IL PSX CLLC - 0394' and product_name='B11';"%(carrierKey)
        # sql="SELECT distinct name FROM rack_analysis.mcmis1 where name like '%"+carrierKey+"%'"
        sql="""SELECT * FROM pdedb.nmfta where scac ="%s" """%(carrierKey)
        dbCursor.execute(sql)
        resultsNmfta=dbCursor.fetchall()

        if carrierKey == None:
            db.close()
            return jsonify({})
        db.close()
        # print resultsMcmis
        return jsonify({"nmfta":resultsNmfta})

    except Exception as e:
        return jsonify({'exception':e})

@app.route('/hazmat/<name>', methods=['POST','GET'])
def get_hazmat(name):
    try:
        carrierKey =  str(name).replace('+',' ').strip()
        db = MySQLdb.connect("172.16.0.55","root","admin123*","rack_analysis")
        dbCursor=db.cursor(MySQLdb.cursors.DictCursor)
        # sql="SELECT distinct supplier,account_type,terminal_name,product_name FROM rack_analysis.supplier_details_contract where supplier='%s' and Account_Type='MANSFIELD OIL CO OF GAINESVILL-10003000UB' and terminal_name='HARTFORD IL PSX CLLC - 0394' and product_name='B11';"%(carrierKey)
        # sql="SELECT distinct name FROM rack_analysis.mcmis1 where name like '%"+carrierKey+"%'"
        sql="""select NAME,NAME_DBA,CEN_NUM FROM `bulk_and_all_class_3_hazmat_car` where NAME = "%s" or NAME_DBA = "%s" """%(carrierKey,carrierKey)
        # sql="""SELECT distinct `name`,`CEN_NUM`,`name_dba` FROM rack_analysis.bulk_and_all_class_3_hazmat_car
        #     WHERE  MATCH (name,name_dba)
        #     AGAINST ('"%s"' IN NATURAL LANGUAGE MODE);"""%(carrierKey)
        dbCursor.execute(sql)
        resultsHazmat=dbCursor.fetchall()

        if carrierKey == None:
            db.close()
            return jsonify({})
        if len(resultsHazmat)==0:
            db.close()
            return jsonify({})
        db.close()
        # print resultsMcmis
        return jsonify({"hazmat":resultsHazmat})

    except Exception as e:
        return jsonify({'exception':e})

@app.route('/hazmat', methods=['POST','GET'])
def get_hazmatQuery():
    try:
        # carrierKey =  str(scac).replace('+',' ').strip()
        carrierKey=urlparse.parse_qs(request.query_string)['q'][0]
        db = MySQLdb.connect("172.16.0.55","root","admin123*","rack_analysis")
        dbCursor=db.cursor(MySQLdb.cursors.DictCursor)
        # sql="SELECT distinct supplier,account_type,terminal_name,product_name FROM rack_analysis.supplier_details_contract where supplier='%s' and Account_Type='MANSFIELD OIL CO OF GAINESVILL-10003000UB' and terminal_name='HARTFORD IL PSX CLLC - 0394' and product_name='B11';"%(carrierKey)
        # sql="SELECT distinct name FROM rack_analysis.mcmis1 where name like '%"+carrierKey+"%'"
        # sql="""select NAME,NAME_DBA,CEN_NUM FROM `bulk_and_all_class_3_hazmat_car` where NAME = "%s" or NAME_DBA = "%s" """%(carrierKey,carrierKey)
        sql="""SELECT distinct `name`,`CEN_NUM`,`name_dba` FROM rack_analysis.bulk_and_all_class_3_hazmat_car
            WHERE  MATCH (name,name_dba)
            AGAINST ('"%s"' IN NATURAL LANGUAGE MODE);"""%(carrierKey)
        dbCursor.execute(sql)
        resultsHazmat=dbCursor.fetchall()

        if carrierKey == None:
            db.close()
            return jsonify({})
        if len(resultsHazmat)==0:
            db.close()
            return jsonify({})
        db.close()
        # print resultsMcmis
        return jsonify({"hazmat":resultsHazmat})

    except Exception as e:
        return jsonify({'exception':e})

@app.route('/hazmatlike/<carrier>', methods=['POST','GET'])
def get_hazmatLike(carrier):
    try:
        carrierKey =  str(carrier).replace('+',' ').strip()
        carrierKey="%"+carrierKey+"%"
        db = MySQLdb.connect("172.16.0.55","root","admin123*","rack_analysis")
        dbCursor=db.cursor(MySQLdb.cursors.DictCursor)
        # sql="SELECT distinct supplier,account_type,terminal_name,product_name FROM rack_analysis.supplier_details_contract where supplier='%s' and Account_Type='MANSFIELD OIL CO OF GAINESVILL-10003000UB' and terminal_name='HARTFORD IL PSX CLLC - 0394' and product_name='B11';"%(carrierKey)
        # sql="SELECT distinct name FROM rack_analysis.mcmis1 where name like '%"+carrierKey+"%'"
        sql="""SELECT distinct `name`,`CEN_NUM`,`name_dba` FROM rack_analysis.bulk_and_all_class_3_hazmat_car
    WHERE  name like "%s" or name_dba like "%s";"""%(carrierKey,carrierKey)
        dbCursor.execute(sql)
        resultsNmfta=dbCursor.fetchall()

        if carrierKey == None:
            db.close()
            return jsonify({})
        db.close()
        finalResult={'HAZMAT': resultsNmfta}
        # print finalResult
        return jsonify(finalResult)

    except Exception as e:
        return jsonify({'exception':e})

@app.route('/hazmatcennum/<cennum>', methods=['POST','GET'])
def get_hazmatDetails(cennum):
    try:
        carrierKey =  str(cennum).replace('+',' ').strip()
        db = MySQLdb.connect("172.16.0.55","root","admin123*","rack_analysis")
        dbCursor=db.cursor(MySQLdb.cursors.DictCursor)
        # sql="SELECT distinct supplier,account_type,terminal_name,product_name FROM rack_analysis.supplier_details_contract where supplier='%s' and Account_Type='MANSFIELD OIL CO OF GAINESVILL-10003000UB' and terminal_name='HARTFORD IL PSX CLLC - 0394' and product_name='B11';"%(carrierKey)
        # sql="SELECT distinct name FROM rack_analysis.mcmis1 where name like '%"+carrierKey+"%'"
        sql="""SELECT * FROM rack_analysis.mcmis1
    WHERE  censusnum="%s";"""%(carrierKey)
        dbCursor.execute(sql)
        resultsMcmis=dbCursor.fetchall()

        if carrierKey == None:
            db.close()
            return jsonify({})
        db.close()
        finalResult={'MCMIS': resultsMcmis}
        # print finalResult
        return jsonify(finalResult)

    except Exception as e:
        return jsonify({'exception':e})

@app.route('/hazmatpc/<cennum>', methods=['POST','GET'])
def get_hazmatDetailsPC(cennum):
    try:
        carrierKey =  str(cennum).replace('+',' ').strip()
        db = MySQLdb.connect("172.16.0.55","root","admin123*","rack_analysis")
        dbCursor=db.cursor(MySQLdb.cursors.DictCursor)
        # sql="SELECT distinct supplier,account_type,terminal_name,product_name FROM rack_analysis.supplier_details_contract where supplier='%s' and Account_Type='MANSFIELD OIL CO OF GAINESVILL-10003000UB' and terminal_name='HARTFORD IL PSX CLLC - 0394' and product_name='B11';"%(carrierKey)
        # sql="SELECT distinct name FROM rack_analysis.mcmis1 where name like '%"+carrierKey+"%'"
        sql="""SELECT select censusnum,
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
usdotrevokedflag FROM rack_analysis.mcmis1
    WHERE  censusnum="%s";"""%(carrierKey)
        dbCursor.execute(sql)
        resultsMcmis=dbCursor.fetchall()

        if carrierKey == None:
            db.close()
            return jsonify({})
        db.close()
        finalResult={'MCMIS': resultsMcmis}
        # print finalResult
        return jsonify(finalResult)

    except Exception as e:
        return jsonify({'exception':e})
if __name__ == '__main__':
    # print "hekkoi"
    # get_task("ADVANTAGE TANK LINE")
    app.run(debug=True)
