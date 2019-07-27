import pandas as pd
import sqlalchemy

########################################################################
PSQL_HOST = "smartapi-db-replica.csgxzpfrma4x.eu-west-1.rds.amazonaws.com"
PSQL_DB = "SmartSolarDB"
PSQL_USER = "SmartSolarUser"
PSQL_PASSWORD = "Bb0xx111PwrRd"
PSQL_PORT = "5432"


########################################################################
class ReplicaDatabase():
    def __init__(self, debug=False):
        self.debug = debug
        self._engine()

    def _engine(self):
        self.engine=sqlalchemy.create_engine("postgresql://"+PSQL_USER+":"+PSQL_PASSWORD+"@"+PSQL_HOST+":"+PSQL_PORT+"/"+PSQL_DB)
        if self.debug: print "Database Engine"

    def query(self, req):
        return pd.read_sql(req, con=self.engine)


def download_table(db, table_name, save_csv=False, save_json=False, debug=False):
    # if debug: print "Getting connection"
    # db = ReplicaDatabase(debug=debug)
    file_name = table_name
    if debug: print file_name

    if table_name == 'state':
        req = "SELECT state_id, product_imei, current_state_type, created_at FROM " + table_name
    elif table_name == 'product':
        req = "SELECT product_imei, imsi, iccid, product_type_id FROM " + table_name
    else:
        req = """
            SELECT * FROM """ + table_name + """

        """

    if debug: print "Requesting data..."
    if debug: print req
    results = db.query(req)

    if save_csv == True:
        # results_csv = pd.DataFrame(results)
        results_csv.to_csv('../tables/'+file_name+'.csv')

    if save_json == True:
        results.to_json(path_or_buf='../tables/'+file_name+'.json', orient='records', date_format='iso', date_unit='s')


def download_sim_table(db, table_name, save_csv=False, save_json=False, debug=False):
    # if debug: print "Getting connection"
    # db = ReplicaDatabase(debug=debug)
    file_name = table_name
    if debug: print file_name

    req = """SELECT
                CASE WHEN pt.name = 'Aeris SIM' THEN 'Aeris'
                    WHEN pt.name = 'Eseye Embedded SIM' THEN 'Eseye'
                    WHEN pt.name = 'Wireless Logic Intelligent Solution SIM' THEN 'Intelligent'
                    WHEN pt.name = 'Vodafone SIM' THEN 'Vodafone'
                    ELSE 'WL'
                END as SIM, ppl.product_imei
                from part_product_linker ppl
                join part p on  ppl.part_id = p.part_id
                join part_type pt on p.part_type_id = pt.part_type_id
                where pt.part_type_category_id = 32
        """

    if debug: print "Requesting data..."
    if debug: print req
    results = db.query(req)

    if save_csv == True:
        # results_csv = pd.DataFrame(results)
        results_csv.to_csv('../tables/'+file_name+'.csv')

    if save_json == True:
        results.to_json(path_or_buf='../tables/'+file_name+'.json', orient='records', date_format='iso', date_unit='s')


