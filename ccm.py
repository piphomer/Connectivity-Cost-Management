import _start_
import requests
import json
import pandas as pd
import os
from os import listdir
from os.path import isfile, join
import re
import sys
from datetime import datetime as dt

import ccm_replica as rep

supplier_list = ['WL','Intelligent','Aeris', 'Eseye']


########################################################################
# Setup some variable assignments
########################################################################

TODAY = pd.to_datetime(dt.now())

# WL & WL INTELLIGENT #

WIRELESS_LOGIC_API_BASE = "https://simpro4.wirelesslogic.com/api/v3/"

BBOXX_ACCOUNT_WL = '110860'
BBOXX_ACCOUNT_INTELLIGENT = '115106'

WL_USERNAME = os.environ['WL_USERNAME']
WL_PASSWORD = os.environ['WL_PASSWORD']

AUTH = (WL_USERNAME,WL_PASSWORD)



# Eseye #

GATEWAY_URL='http://eseye-gateway.bboxx.co.uk'
AUTH_TOKEN = 'vykSuTvgZI6OJFWiIOkeXC70T4IdBeRV'
HEADERS = {'Content-Type': 'application/json'}

class GatewayAuth(requests.auth.AuthBase):
    def __init__(self, token):
        self._token = token

    def __call__(self, r):
        # modify and return the request
        r.headers['X-Auth'] = self._token
        return r


########################################################################
# Common functions
########################################################################

def make_df(data):
        
    results = pd.DataFrame(data)

    return results

########################################################################

def get_tables():
    db = rep.ReplicaDatabase()
    # rep.download_table(db, 'product', save_json = True, debug = True)
    # rep.download_table(db, 'state', save_json=True, debug=True)
    # rep.download_table(db, 'entity', save_json=True, debug=True)
    # rep.download_table(db, 'product_entity_linker', save_json=True, debug=True)
    # rep.download_table(db, 'state_type', save_json=True, debug=True)

    print "Opening Products table"
    with open('tables/product.json', 'rb') as rf:
        products_df = make_df(json.load(rf))
        print products_df.shape[0], "products found in SmartSolar"

    print "Opening States table"
    with open('tables/state.json', 'rb') as rf:
        states_df = make_df(json.load(rf))
        states_df = states_df[['state_id','product_imei','current_state_type','created_at']]
        states_df.created_at = pd.to_datetime(states_df.created_at)
        states_df.product_imei = states_df.product_imei.astype('str')
        #states_df.set_index('product_imei', inplace=True)

    print "Opening Entities table"
    with open('tables/entity.json', 'rb') as rf:
        entities_df = make_df(json.load(rf))
        entities_df = entities_df[['entity_id','name']]
        entities_df.rename(index=str, columns = {'name': 'entity'}, inplace=True)

    print "Opening Product/Entity Linker table"
    with open('tables/product_entity_linker.json', 'rb') as rf:
        pel_df = make_df(json.load(rf))
        pel_df = pel_df[['product_imei','entity_id','date_added','date_removed']]

    print "Opening State Type table"
    with open('tables/state_type.json', 'rb') as rf:
        state_types_df = make_df(json.load(rf))
        state_types_df = state_types_df[['state_type_id', 'name']]
        state_types_df.rename(index=str, columns = {'name': 'state'},inplace=True)

    return products_df, states_df, entities_df, pel_df, state_types_df

########################################################################

def make_invoice_path(supplier):
    invoice_path = './invoices/{}/csv/'.format(supplier)
    return invoice_path

def make_invoice_filename(invoice):
    invoice_filename = invoice['date'] + "_" + invoice['invref'] + ".csv"

    return invoice_filename

########################################################################

def make_report_filename(invoice, supplier):

    print invoice
    #Create a datetime object from the date value of the invoice
    try:
        dateobject = dt.strptime(invoice['date'], "%Y%m")
    except:
        dateobject = dt.strptime(invoice['date'], "%Y%m%d")
    

    #Generate a nicer date string to name the report by
    report_filename = dt.strftime(dateobject, "%Y-%m (%b)") + " " + supplier + " Cost Report.csv"

    print report_filename
    
    return report_filename

########################################################################

def make_grouped_report_filename(month, supplier):

    #Create a datetime object from the date value of the invoice
    try:
        dateobject = dt.strptime(invoice['date'], "%Y%m")
    except:
        dateobject = dt.strptime(month, "%Y%m%d")
        
    #Generate a nicer date string to name the report by
    grouped_report_filename = "./CCM Reports/" + dt.strftime(dateobject, "%Y-%m (%b)") + " - {} - CCM Report.csv".format(supplier)
    
    return grouped_report_filename



def download_new_invoices(supplier):

    if supplier == 'WL' or supplier == 'Intelligent':
        #Get list of all available invoices
        if supplier == 'WL':
            URL = WIRELESS_LOGIC_API_BASE + "invoices?_format=json&billing-account=" + BBOXX_ACCOUNT_WL
        else:
            URL = WIRELESS_LOGIC_API_BASE + "invoices?_format=json&billing-account=" + BBOXX_ACCOUNT_INTELLIGENT

        try: #if internet connection exists

            r = requests.get(url=URL, auth=AUTH)

            print r.status_code

            if r.status_code == 200:
                print "Invoice list succesfully downloaded."
            else:
                print "Need to add an error handler!"
                return

            r_json = r.json()

            invoice_list = []

            for invoice in r_json:
                
                #invref = invoice['invref'].replace("/","-") + ".csv"
                invref = invoice['invref'].replace("/","-")
                invdate = invoice['date'].replace("-","")

                invdict = {'invref': invref, 'date' : invdate}
                invoice_list.append(invdict)
            
            print invoice_list
            return invoice_list

        except: #if no internet connection exists, just look at already downloaded invoices

            invoice_path = make_invoice_path(supplier)

            interim_list = [f for f in listdir(invoice_path) if isfile(join(invoice_path, f))]

            invoice_list = []

            for invoice in interim_list:

                #split the invoice file name into date and reference number
                invdate, invref = invoice.split("_")

                #Not needed for WL right now but maybe I will generalise this function later
                invref = invref.replace(".csv","")
                        
                invdict = {'invref': invref, 'date' : invdate}
                invoice_list.append(invdict)

            return invoice_list
        #Check which ones are not already downloaded

        #Download new ones

    if supplier == 'Aeris' or supplier == 'Eseye':
        print "{} invoices have to be downloaded manually for now."

    return

####################################################################################################

def get_invoice_list(supplier):

    ### Wireless Logic and WL Intelligent ###

    if supplier == 'WL' or supplier == 'Intelligent':
    
        # Get list of available invoices
        if supplier == 'WL':
            URL = WIRELESS_LOGIC_API_BASE + "invoices?_format=json&billing-account=" + BBOXX_ACCOUNT_WL
        else:
            URL = WIRELESS_LOGIC_API_BASE + "invoices?_format=json&billing-account=" + BBOXX_ACCOUNT_INTELLIGENT

        try: #if internet connection exists

            r = requests.get(url=URL, auth=AUTH)

            print r.status_code

            if r.status_code == 200:
                print "Invoice list succesfully downloaded."
            else:
                print "Need to add an error handler!"
                return

            r_json = r.json()

            invoice_list = []

            for invoice in r_json:
                
                #invref = invoice['invref'].replace("/","-") + ".csv"
                invref = invoice['invref'].replace("/","-")
                invdate = invoice['date'].replace("-","")

                invdict = {'invref': invref, 'date' : invdate}
                invoice_list.append(invdict)
            
            return invoice_list

        except: #if no internet connection exists, just look at already downloaded invoices

            invoice_path = make_invoice_path(supplier)

            interim_list = [f for f in listdir(invoice_path) if isfile(join(invoice_path, f))]

            invoice_list = []

            for invoice in interim_list:

                #split the invoice file name into date and reference number
                invdate, invref = invoice.split("_")

                #Not needed for WL right now but maybe I will generalise this function later
                invref = invref.replace(".csv","")
                        
                invdict = {'invref': invref, 'date' : invdate}
                invoice_list.append(invdict)

            return invoice_list



    ### Aeris ####

    if supplier == 'Aeris':

        # Right now this will have to be a manual download into this folder
        invoice_path = make_invoice_path(supplier)
        interim_list = [f for f in listdir(invoice_path) if isfile(join(invoice_path, f))]


        invoice_list = []

        for invoice in interim_list:

            print invoice

            #split the invoice file name into date and reference number
            try:
                invdate, invref = invoice.split("_")
            except:
                print "Invoice may be incorrectly named (check for hyphen instead of underscore?)"

            #remove the .csv extension
            invref = invref.replace(".csv","")
                    
            invdict = {'invref': invref, 'date' : invdate}
            invoice_list.append(invdict)

            #print invdict

        print invoice_list
        
        return invoice_list

    ### Eseye ###

    if supplier == 'Eseye':
        # Right now this will have to be a manual download into this folder
        invoice_path = make_invoice_path(supplier)
        
        interim_list = [f for f in listdir(invoice_path) if isfile(join(invoice_path, f))]

        invoice_list = []

        for invoice in interim_list:

            #split the invoice file name into date and reference number
            invdate, invref = invoice.split("_")

            #remove the .csv extension
            invref = invref.replace(".csv","")
            
            invdict = {'invref': invref, 'date' : invdate}
            invoice_list.append(invdict)
        
        return invoice_list

####################################################################################################

def get_new_invoices(invoice_list, supplier):
    #This function downloads invoices from each supplier via their respective APIs

    if supplier == 'WL' or supplier == 'Intelligent':

        # Check to see if locally stored copy exists, if not, download missing ones
        for invoice in invoice_list:

            invoice_filename = make_invoice_filename(invoice)
            invoice_path = make_invoice_path(supplier)
            
            if os.path.isfile(invoice_path+invoice_filename):
                print "Invoice " + invoice['invref'] + " already downloaded." 
                
            else:

                print "Downloading invoice " + invoice['invref'] + " from",

                URL = WIRELESS_LOGIC_API_BASE + "invoices/" + invoice['invref'] + "?_format=json"

                print URL

                r = requests.get(url=URL, auth=AUTH)

                if r.status_code == 200:
                    print "complete!"

                if r.status_code == 500:
                    print "Bummer. WL API cannot retrieve this invoice at this time"
                    return

                r_json = make_df(r.json())

                print invoice_path + invoice_filename

                r_json.to_csv(invoice_path + invoice_filename)

        return

    if supplier == 'Aeris':
        pass #Manually downloading invoices for now

    if supplier == 'Eseye':
        pass #Manually downloading invoices for now

####################################################################################################

def check_reports(invoice, supplier):

    grouped_report_filename = make_grouped_report_filename(invoice['date'], supplier)

    #Check if report already exists
    if os.path.isfile(grouped_report_filename):
        return True
    else:
        return False

def join_invoice_to_product_entity(supplier, invoice_df):
    #This function adds the data from the product_entity_df dataframe to the invoice dataframe

    if supplier == 'WL':
        #WL not processed this way
        pass

    if supplier == 'Aeris':

        #Match Aeris SIMs on IMSI
        #Force imsi columns in each table to be same type else they won't join
        product_entity_df.imsi = product_entity_df.imsi.astype(str)
        invoice_df.imsi = invoice_df.imsi.astype(str)

        #Join entity table to sim table
        supplier_product_entity = invoice_df.merge(product_entity_df, how="left", on="imsi")

        return supplier_product_entity

    elif supplier == 'Eseye':
        #Match Eseye SIMs on ICCID
        #Force iccid columns in each table to be same type else they won't join
        product_entity_df.iccid = product_entity_df.iccid.astype(str)
        ESEYE_SIMS.iccid = ESEYE_SIMS.iccid.astype(str)

        #Join entity table to sim table
        supplier_product_entity = ESEYE_SIMS.merge(product_entity_df, how="left", on="iccid")

        return supplier_product_entity

    else:
        pass


def create_grouped_report(invoice, supplier, currency = 'GBP'):

    grouped_report_filename = make_grouped_report_filename(invoice['date'], supplier)

    #Group the results by entity
    grouped_df = report_df[['entity_name','tariff_name','rental','data','sms','total']]
    # print grouped_df.head(50)
    # print grouped_df['total'].sum()

    grouped_df = grouped_df.groupby(['entity_name','tariff_name'], \
                    axis = 0).agg({'rental': 'sum','data': 'sum','sms': 'sum','total' : ['sum', 'count']})
    # print grouped_df.head(50)
    # print grouped_df['total'].sum()

    #Flatten the column indexes
    grouped_df = grouped_df.reset_index()
    grouped_df.columns = [' '.join(col).strip() for col in grouped_df.columns.values]

    #Rearrange the columns
    grouped_df = grouped_df[['entity_name','tariff_name','total count','rental sum','sms sum','data sum','total sum']]

    #Rename the columns
    grouped_df = grouped_df.rename(index=str, columns={'total count': 'count',
                                                        'rental sum': 'rental',
                                                        'sms sum': 'sms',
                                                        'data sum': 'data',
                                                        'total sum': 'total'})

    # print grouped_df['total'].sum()

    #Add a column with the invoice month to filter on in Power BI
    try:
        report_month = dt.strftime(dt.strptime(invoice['date'], "%Y%m"), "%m-%Y")
    except:
        report_month = dt.strftime(dt.strptime(invoice['date'], "%Y%m%d"), "%m-%Y")
    
    grouped_df['month'] = report_month

    #Add a column showing the supplier
    grouped_df['supplier'] = supplier

    #Add a column showing that WL costs are in USD
    grouped_df['currency'] = currency

    
    #Set the index column
    grouped_df.set_index('entity_name', inplace=True)

    #Rearrange the columns of the grouped report
    grouped_df = grouped_df[['supplier','tariff_name','count','rental','sms','data','total','month','currency']]

    #Output the file
    print "Generating report for invoice " + invoice['invref']
    grouped_df.to_csv(grouped_report_filename)
    

####################################################################################################


####################################################################################################
# WL Specific Functions
####################################################################################################

def get_df_of_wl_sims():

    #There seems to be a problem with the WL API
    #It returns all BBOXX SIMs in both accounts when called only on one account.
    #So we'll process all on the WL loop here

    print "Getting list of WL SIMs, including Intelligent"

    URL = WIRELESS_LOGIC_API_BASE + "sims?_format=json&billing-account=" + BBOXX_ACCOUNT_WL
    
    r = requests.get(url=URL, auth=AUTH)

    r_json = r.json()['sims']

    #Make a dataframe with all (WL and Intelligent) SIMs
    wl_sims_df = make_df(r_json)

    print "Number of WL + Intelligent SIMs: {}".format(len(wl_sims_df))

    wl_sims_df.to_csv('WL_sims.csv')

    return wl_sims_df

####################################################################################################

def get_df_of_wl_tariffs():

    # #Just take the first 'x' entries while testing to minimise run time
    # sims_df = sims_df.head(900)

    #convert to list
    wl_sims_list = wl_sims_df['iccid'].tolist()

    #set up empty list
    wl_tariffs_list = []

    #Can only call about 350 iccid's per call before the URL becomes too long
    iccids_per_call = 350

    page_count = len(wl_sims_list) / iccids_per_call       #Calculate how many pages we need iterate through


    print "Retrieving tariffs and tariff details..."

    for page in range(page_count + 1):

        print page

        #empty the string
        iccid_string = ""

        #iccid to start with on this call
        start_iccid = page * iccids_per_call
        
        for number in range(iccids_per_call):
            try:
                iccid_string = iccid_string +"," + wl_sims_list[start_iccid + number]
            except:
                print "Error in generating the iccid string to send to the API!"
                print "These are probably just the last page phantom ones"

        URL = WIRELESS_LOGIC_API_BASE + "sims/details?_format=json&iccid=" + iccid_string

        percent_complete = (float(page) / page_count * 100)

        print "\r" + str(int(percent_complete)) + "% complete",

        r = requests.get(url=URL, auth=AUTH)

        #print r.status_code

        r_json = r.json()

        #print json.dumps(r_json, indent=4, sort_keys=True)


        for index, json_data in enumerate(r_json):

            try:
                iccid = r_json[index]['iccid']
                tariff_name = r_json[index]['active_connection']["customer_tariff"]["name"]
                mno_account  = r_json[index]['mno_account']['mno']['name']
            except:
                iccid = r_json[index]['iccid']
                tariff_name = "INACTIVE"
                mno_account = "N/A"

            this_list = [iccid, tariff_name, mno_account]

            wl_tariffs_list.append(this_list)

    #Turn the list into a dataframe
    wl_tariffs_df = make_df(wl_tariffs_list)

    print wl_tariffs_df.head()

    #Name the columns
    wl_tariffs_df.columns = ['iccid','tariff_name','mno_account']

    #Now,
    #wl_sims_df contains a set of sims: iccid, id, imsi, msisdn, status, workflow_status
    #wl_tariffs_df contains the iccids of those SIMs plus their associated tariff name and mno account
    #So now merge the two:
    wl_sims_plus_tariffs_df = wl_sims_df.merge(wl_tariffs_df, on="iccid")

    #Make the iccid the index
    wl_sims_plus_tariffs_df.set_index('iccid', inplace=True)

    #Resulting dataframe contains account details for ALL WL SIMs including Intelligent
    print "Writing csv file..."
    wl_sims_plus_tariffs_df.to_csv('wl_sims_plus_tariffs.csv')
    print "Success!"

    return wl_sims_plus_tariffs_df

####################################################################################################

# This function takes the product_entity_df (containing details of which entity each product is 
# assigned to) and adds the tariff information from previous function

def wl_add_product_entity_and_ctn(supplier):

    updateWSPE = raw_input("make new WL_SIMS_PRODUCT_ENTITY?") #debug. Delete later

    
    if supplier == "WL":

        if updateWSPE == "y":             

            #Copy the product_entity_df into a new df and take product_imei out of the index
            wl_product_entity_df = product_entity_df.reset_index()

            #Get rid of iccid's from wl_product_entity_df as they are anyway present in the invoice
            #and we aren't matching on them
            wl_product_entity_df.drop(['iccid'], axis=1, inplace=True)

            #Join the product_entity dataframe to the wl_sims_plus_tariffs dataframe
            wl_sims_product_entity_df = wl_sims_plus_tariffs_df.merge(wl_product_entity_df, how="left", on="imsi")

            #Drop the Intelligent SIMs from this dataframe
            wl_sims_product_entity_df = wl_sims_product_entity_df[wl_sims_product_entity_df.mno_account != 'intelligent']
                                   
            # Create a CTN column in order to be able to join to the invoices
            # Because the WL invoices only give costs against CTN, not ICCID or any other fixed reference
            # - if mno account is Vodafone Global, ctn = imsi
            # - otherwise ctn = msisdn

            print "Adding CTNs to dataframe..."

            def add_ctn(wl_sims_product_entity_df):
                if wl_sims_product_entity_df['mno_account'] == 'Vodafone Global':
                    wl_sims_product_entity_df['ctn'] = wl_sims_product_entity_df['imsi']
                else:
                    wl_sims_product_entity_df['ctn'] = wl_sims_product_entity_df['msisdn']

                return wl_sims_product_entity_df

            # Run the .apply function over the dataframe
            wl_sims_product_entity_df = wl_sims_product_entity_df.apply(add_ctn,axis=1)

            #Set iccid to be index
            wl_sims_product_entity_df.set_index('iccid', inplace=True)

            wl_sims_product_entity_df.to_csv("wl_sims_product_entity.csv")


        else:
            wl_sims_product_entity_df = pd.read_csv("wl_sims_product_entity.csv",
                                                    dtype={"imsi": str, "msisdn": str, "iccid": str})
        
        return wl_sims_product_entity_df

    elif supplier == "Intelligent":

        if updateWSPE == "y":

            intelligent_product_entity_df = product_entity_df.reset_index().drop(['index'], axis=1)

            intelligent_product_entity_df.drop(['imsi'], axis=1, inplace=True)

            #Only keep entries that are intelligent SIMs
            #intelligent_sims_plus_tariffs_df = wl_sims_plus_tariffs_df[wl_sims_plus_tariffs_df.mno_account == "intelligent"]
            intelligent_sims_plus_tariffs_df = wl_sims_plus_tariffs_df[wl_sims_plus_tariffs_df.mno_account != 'O2 Global']
            intelligent_sims_plus_tariffs_df = intelligent_sims_plus_tariffs_df[intelligent_sims_plus_tariffs_df.mno_account != 'Vodafone Global']

            intelligent_sims_plus_tariffs_df = intelligent_sims_plus_tariffs_df.reset_index()

            #Merge product_entity to intelligent_sims_plus_tariffs
            intelligent_sims_product_entity_df = pd.merge(intelligent_sims_plus_tariffs_df,
                                                                    intelligent_product_entity_df, on="iccid", how="left")

            #Add a CTN which is just the MSISDN, to join to the invoice on
            intelligent_sims_product_entity_df['ctn'] = intelligent_sims_product_entity_df['msisdn']

            #Set iccid to be index
            intelligent_sims_product_entity_df.set_index('iccid', inplace=True)

            #Drop a bunch of unwanted flotsam including the IMSI which is not used on Intelligent
            intelligent_sims_product_entity_df.drop(['index', 'imsi'], axis=1, inplace=True)

            #Output to csv
            intelligent_sims_product_entity_df.to_csv("intelligent_sims_product_entity.csv")

        else:
            intelligent_sims_product_entity_df = pd.read_csv("intelligent_sims_product_entity.csv", dtype = {'ctn': str})
        
        return intelligent_sims_product_entity_df

    else:
        print "Well, this is awkward..."

####################################################################################################

def create_wl_report(supplier, invoice):

    invoice_filename = make_invoice_filename(invoice)
    invoice_path = make_invoice_path(supplier)

    report_filename = make_report_filename(invoice, supplier)
    report_path = './Full Reports/{}/'.format(supplier)

    #grouped_report_file = grouped_report_filename(invoice)

    #Read the invoice file
    try:
        #report_df = pd.read_csv(invoice_path + invoice_filename, dtype={'ctn': str})
        report_df = pd.read_csv(invoice_path + invoice_filename)
    except:
        print "Invoice {} does not seem to exist! Skipping...".format(invoice_filename)
        return

    #Just keep the columns we're interested in
    report_df = report_df[['ctn','rental','gprs','gprs_usage','gprsroam',
                            'sms','sms_usage','smsroam','nettotal']]

    #Ensure CTN in both df's is the same dtype, just in case
    report_df.ctn = report_df.ctn.astype('str')
    wtf_to_call_this_df.ctn = wtf_to_call_this_df.ctn.astype('str')

    #Join the invoice(report_df) to wl_sims_product_entity_df
    report_df = report_df.merge(wtf_to_call_this_df, on="ctn", how="left")
    

    #If entity field is blank, product must have been deleted from Smart Solar
    report_df = report_df.fillna(value = {'entity_id': 0, 'name': 'Product not in SmartSolar'})

    #Sum the gprs + gprs_roam values and sms + sms_roam values
    report_df['gprs'] = report_df['gprs'] + report_df['gprsroam']
    report_df['sms'] = report_df['sms'] + report_df['smsroam']

    #Just pull the fields we are interested in
    report_df = report_df.drop(['date_added','date_removed','id','msisdn','workflow_status','smsroam','gprsroam'], axis=1)



    #Rename a couple of columns
    report_df = report_df.rename(index=str, columns={'nettotal': 'total',
                                                        'name': 'entity_name',
                                                        'gprs': 'data',
                                                        'gprs_usage': 'data_usage'})
    #Itemised report
    report_df.to_csv(report_path+report_filename)

    return report_df


####################################################################################################
# Aeris Specific Functions
####################################################################################################

def process_aeris_invoice(supplier, invoice):
    #This function reads the (pre-saved) invoice and gets rid of unwanted data in it

    invoice_path = make_invoice_path(supplier)
    invoice_filename = make_invoice_filename(invoice)

    #Read the invoice file
    invoice_df = pd.read_csv(invoice_path + invoice_filename)

    # Just keep the columns we're interested in
    invoice_df = invoice_df[['IMSI',
                            'ICCID',
                            'BILL PERIOD',
                            'MSISDN',
                            'TOTAL MONTHLY CHARGES',
                            'TOTAL DEVICE CHARGES',
                            'BILL STATUS',
                            'RATE PLAN NAME',
                            'HOME ZONE',
                            'ZONE_NAME',
                            'POOL NAME',
                            'ACCESS FEE PRICE',
                            'PLAN INCLUDED PKT KB',
                            'NON-BILL STATUS SMS MT MSGS',
                            'NON-BILL STATUS PKT KB',
                            'BILL SMS MT MSGS',
                            'BILL SMS MO TRAFFIC CHARGES',
                            'BILL SMS MT TRAFFIC CHARGES',
                            'BILL PKT KB',
                            'INCLUDED PKT BYTES',
                            'BILL PKT TRAFFIC CHARGES',
                            'BILL TOTAL TRAFFIC CHARGES',
                            'PRORATED ACCESS FEE']]


    #lower case some columns
    invoice_df.rename(columns = {'ICCID': 'iccid', 'IMSI':'imsi'}, inplace=True)

    #Get rid of any extraneous punctuation in the invoice data (e.g. =")
    def strip_extraneous(invoice_df):
        invoice_df['iccid'] = re.findall("\d+", invoice_df['iccid'])[0] #re.findall returns a list
        invoice_df['imsi'] = re.findall("\d+", invoice_df['imsi'])[0]
        invoice_df['MSISDN'] = re.findall("\d+", invoice_df['MSISDN'])[0]
        invoice_df['RATE PLAN NAME'] = re.findall("\d+", invoice_df['RATE PLAN NAME'])[0]
        return invoice_df

    invoice_df = invoice_df.apply(strip_extraneous, axis=1)
    
    #Drop any rows where 'BILL STATUS' is 'PROV'
    invoice_df = invoice_df[invoice_df['BILL STATUS'] != 'PROV']

    #Drop any rows where 'RATE PLAN NAME' is '90DAYSTRIAL_GLOBAL_10MB' or '90DAYSTRIAL_GLOBAL'
    invoice_df = invoice_df[invoice_df['RATE PLAN NAME'] != '90DAYSTRIAL_GLOBAL_10MB']
    invoice_df = invoice_df[invoice_df['RATE PLAN NAME'] != '90DAYSTRIAL_GLOBAL']
    
    return invoice_df

####################################################################################################


def create_aeris_report(supplier, invoice, report_df):
    #This function processes the invoice + entity dataframe and generates a grouped report

    # invoice_filename = make_invoice_filename(invoice)
    report_filename = make_report_filename(invoice, supplier)    
    report_path = './Full Reports/{}/'.format(supplier)
    
    #If entity field is blank, SIM has likely been provisioned but not put in a product yet
    report_df = report_df.fillna(value = {'entity_id': 0, 'name': 'Product not in SmartSolar'})
    
    #Just pull the fields we are interested in
    report_df = report_df.drop(['date_added','date_removed','iccid_y'], axis=1)
    
    #lower case the ICCID column
    report_df.rename(columns = {'iccid_x': 'iccid'}, inplace=True)

    #Drop any totally duplicated rows because duplicated rows exist in SmartSolar
    report_df.drop_duplicates(inplace=True)

    #Group by ItemRef, to consolidate multiple items of same type
    report_df = report_df.groupby(['imsi'], axis = 0).agg({'iccid': 'first',
                                                            'BILL PERIOD': 'first',
                                                            'MSISDN': 'first',
                                                            'TOTAL MONTHLY CHARGES': sum,
                                                            'TOTAL DEVICE CHARGES': sum,
                                                            'BILL STATUS': 'first',
                                                            'RATE PLAN NAME': 'first',
                                                            'HOME ZONE': 'first',
                                                            'ZONE_NAME': 'first',
                                                            'POOL NAME': 'first',
                                                            'ACCESS FEE PRICE': 'first',
                                                            'NON-BILL STATUS SMS MT MSGS': sum,
                                                            'NON-BILL STATUS PKT KB': sum,
                                                            'BILL SMS MT MSGS': sum,
                                                            'BILL PKT KB': sum,
                                                            'BILL SMS MO TRAFFIC CHARGES': sum,
                                                            'BILL SMS MT TRAFFIC CHARGES': sum,
                                                            'INCLUDED PKT BYTES': sum,
                                                            'BILL PKT TRAFFIC CHARGES': sum,
                                                            'BILL TOTAL TRAFFIC CHARGES': sum,
                                                            'PRORATED ACCESS FEE': sum,
                                                            'product_imei': 'first',
                                                            'entity_id': 'first',
                                                            'name': 'first'})

    report_df = report_df.reset_index()

    #Rename a couple of columns
    report_df = report_df.rename(index=str,
                        columns={'name': 'entity_name',
                                'RATE PLAN NAME': 'tariff_name',
                                'TOTAL DEVICE CHARGES': 'total',
                                'TOTAL MONTHLY CHARGES': 'rental',
                                'BILL SMS MO TRAFFIC CHARGES': 'sms_mo',
                                'BILL SMS MT TRAFFIC CHARGES': 'sms_mt',
                                'BILL PKT TRAFFIC CHARGES': 'data'})

    #Add a column with total sms cost for each imsi
    report_df['sms']=report_df[['sms_mo','sms_mt']].sum(1)

    # #Create the grouped report
    # create_grouped_report(invoice, 'Aeris', 'GBP')

    return report_df


####################################################################################################
# Eseye Specific Functions
####################################################################################################

def get_eseye_sims_by_page(start, num):
    url = "{}/get_sim_list".format(GATEWAY_URL)

    params = {
        'state': 'provisioned',
        'start_rec': start,
        'num_recs': num
    }

    r = requests.get(url=url,
                     params=params,
                     headers=HEADERS,
                     auth=GatewayAuth(AUTH_TOKEN))

    return r.json()['data']['sims']
    #return r.json()


def get_eseye_sims():

    sims = []

    sims_per_page = 10

    while True:

        print len(sims)

        page = get_eseye_sims_by_page(len(sims), sims_per_page)
        
        if len(page) == 0:
            break

        sims.extend(page)

    sims_df = make_df(sims)
    print sims_df
    sims_df.to_csv('eseye_sims.csv')    

    print "Total SIMs returned: {}".format(len(sims))

    return sims_df


def create_eseye_report(supplier, invoice, product_entity):

    invoice_filename = make_invoice_filename(invoice)
    invoice_path = make_invoice_path(supplier)
    
    print invoice_path + invoice_filename

    #Read the invoice file
    report_df = pd.read_csv(invoice_path + invoice_filename, skiprows=[0],index_col=False)

    print report_df.head(50)

    # Just keep the columns we're interested in
    report_df = report_df[['ItemRef','ICCID','PackageID','Quantity','Currency','Rate','Amount']]

    #lower case the ICCID column
    report_df.rename(columns = {'ICCID': 'iccid'}, inplace=True)

    #strip any '' off certain fields
    def strip_apostrophe(report_df):
        report_df['iccid'] = report_df['iccid'].strip("'")
        report_df['PackageID'] = report_df['PackageID'].strip("'")
        return report_df

    report_df = report_df.apply(strip_apostrophe, axis=1)

    # Add the entity information from the product_entity dataframe
    report_df = report_df.merge(product_entity, on="iccid", how="left")

    #Just keep the columns we're interested in
    report_df = report_df[['ItemRef','iccid','PackageID','Quantity','Currency','Rate','Amount','status',
                                    'entity_id','name','imsi','product_imei']]

    #If entity field is blank, SIM has likely been provisioned but not put in a product yet
    report_df = report_df.fillna(value = {'entity_id': 0, 'name': 'Product not in SmartSolar'})

    #Group by ItemRef, to consolidate multiple items of same type
    report_df = report_df.groupby(['ItemRef','iccid'], axis = 0).agg({'PackageID': 'first',
                                                                        'Quantity': sum,
                                                                        'Currency': 'first',
                                                                        'Rate': sum,
                                                                        'Amount': sum,
                                                                        'status': 'first',
                                                                        'entity_id': 'first',
                                                                        'name': 'first',
                                                                        'imsi': 'first',
                                                                        'product_imei': 'first'})
    
    report_df = report_df.reset_index()

    #Create a new dataframe with only SMS costs
    apismsmt_df = report_df[report_df.ItemRef == 'APISMSMT'][['iccid','Amount','Quantity','Rate']]
    sms_df = report_df[report_df.ItemRef == 'SMS'][['iccid','Amount','Quantity','Rate']]
    
    apismsmt_df.rename(columns={'Amount': 'apismsmt','Quantity':'APISMSMTQuantity','Rate':'APISMSMTRate'}, inplace=True)
    sms_df.rename(columns={'Amount': 'sms','Quantity':'SMSQuantity','Rate':'SMSRate'}, inplace=True)
    
    sms_df = sms_df.merge(apismsmt_df, how='outer', on='iccid')
    sms_df['sms'] = sms_df[['sms','apismsmt']].sum(1)
    sms_df['SMSQuantity'] = sms_df[['SMSQuantity','APISMSMTQuantity']].sum(1)
    sms_df['SMSRate'] = sms_df[['SMSRate','APISMSMTRate']].sum(1)


    #Create a new dataframe with only data costs
    data_df = report_df[report_df.ItemRef == 'Data'][['iccid','Amount','Quantity','Rate']]
    data_df.rename(columns={'Amount': 'data','Quantity':'DataQuantity','Rate':'DataRate'}, inplace=True)

    #Create a new dataframe with only service (rental) costs
    service_df = report_df[report_df.ItemRef == 'Service']
    service_df.rename(columns={'Amount': 'rental','Quantity':'ServiceQuantity','Rate':'ServiceRate'}, inplace=True)

    #Merge all the dataframes
    report_df = service_df.merge(data_df, how='outer', on='iccid')
    report_df = report_df.merge(sms_df, how='outer', on='iccid')

    #Add a column with total cost for each iccid
    report_df['total']=report_df[['sms','data','rental']].sum(1)

    #Rename a couple of columns
    report_df = report_df.rename(index=str,
                        columns={'name': 'entity_name',
                                'PackageID':'tariff_name',
                                'ServiceAmount': 'rental'})

    #Any rows that don't have an entity assigned at this point must be non-SIM invoice items
    #i.e. costs associated with SIMS that aren't on the SIM list returned by API
    #Assume for now that these are unassignable SMS costs as in April 18 invoice
    report_df = report_df.fillna(value = {'entity_name': 'non-entity cost', 'tariff_name': '9016'})
  
    return report_df


####################################################################################################
####################################################################################################
####################################################################################################


if __name__ == '__main__':

    #Check for new invoices via the suppliers' APIs
    for supplier in supplier_list:

        print "Looking for new invoices from " + supplier

        download_new_invoices(supplier)

    #Make the list of months for which to create reports
    date_list = pd.date_range('2014-01-01', TODAY, freq='MS').tolist()

    #Turn the list into a dataframe
    dates_df = pd.DataFrame(date_list, columns=['start_date'])

    #Make the end date of each month be the start date of the following month
    dates_df['end_date'] = dates_df['start_date'].shift(-1)

    #We're not interested in this (i.e. current) month so just drop the last row. It doesn't have an end date anyway
    dates_df = dates_df[:-1]


    #Loop through all the months in the month list
    for i, month in enumerate(dates_df.start_date):

        print dt.strftime(month, '%y-%b')

        month_start = month
        month_end = dates_df.end_date[i]
        
        #Get the product_entity_state_df dataframe for this month
        pes_filename = "product_entity_state/{}_product_entity_state.csv".format(dt.strftime(month_start, '%y-%b'))
        
        if os.path.isfile(pes_filename):
            product_entity_state_df = pd.read_csv(pes_filename)
        else:
            #Get the tables from SmartSolar
            #But this is horribly inefficient, opening them for each month
            #But but... this should not need to be done for more than one month per run anyway... two tops!
            products_df, states_df, entities_df, pel_df, state_types_df = get_tables()
            
            #Create a bespoke product_entity_state_df for this month
            product_entity_state_df = make_product_entity_state_df(month_start, month_end)

            product_entity_state_df.to_csv(pes_filename)


        #Workflow is different for each SIM provider

        for supplier in supplier_list:

            print "Working on " + supplier

            # #Get list of available invoices
            # #print "Getting invoice list..."
            # #invoice_list = get_invoice_list(supplier)

            # invoice_fname prototype:
            #      201701_invoice_Aeris.csv
            #      201802_invoice_WL.csv
            #      etc

            invoice_path = make_invoice_path(supplier)
            invoice_fname = "{}_invoice_{}.csv".format(dt.strftime(month, '%Y%m'),supplier)

            #print os.path.join(invoice_path,invoice_fname)

            #Get invoice for this month
            if supplier == 'WL' or supplier == 'Intelligent':

                if os.path.isfile(os.path.join(invoice_path,invoice_fname)):
                    invoice_df = pd.read_csv(os.path.join(invoice_path,invoice_fname))
                else:
                    print "Invoice does not exist for this month for {}".format(supplier)

            else:
                print "2"

            print invoice_df.head(10)
            raw_input("?")

            # Download any new invoices
            get_new_invoices(invoice_list, supplier)


            if supplier == 'WL' or supplier == 'Intelligent':

                wl_sims_file = "WL_sims_plus_tariffs.csv" #this file contains all WL SIMs, including Intelligent ones

                #Get complete list of SIMs + tariffs they are on
                if os.path.isfile(wl_sims_file):
                    update_sims_df = raw_input("Do you want to update the WL SIM list (Y)? (Warning: will take several hours to run): ")
                else:
                    update_sims_df = "Y"

                if update_sims_df == "Y" or update_sims_df == "y":

                    #Get complete list of WL SIMs including the Intelligent SIMs
                    wl_sims_df = get_df_of_wl_sims()

                    #Get tariff details
                    wl_sims_plus_tariffs_df = get_df_of_wl_tariffs()

                try:
                    wl_sims_plus_tariffs_df
                except NameError:
                    print "Reading SIM list from file..."
                    wl_sims_plus_tariffs_df = pd.read_csv(wl_sims_file, dtype={"imsi": str, "msisdn": str})

                #This df will have all WL and Intelligent SIMs in it
                wtf_to_call_this_df = wl_add_product_entity_and_ctn(supplier)
                

                for invoice in invoice_list:

                    print "Invoice number: " + invoice['invref']

                    # Check if report already exists 
                    report_exists = check_reports(invoice, supplier)
                    
                    if report_exists:
                        print "Report already exists for invoice " + invoice['invref']
                    else:
                        print "Creating report for invoice " + invoice['invref']

                        #Create the full report
                        report_df = create_wl_report(supplier, invoice)

                        #Create the grouped report
                        create_grouped_report(invoice, supplier, 'GBP')


            if supplier == 'Aeris':
                    
                for invoice in invoice_list:

                    print "Invoice number: " + invoice['invref']

                    # Check if report already exists 
                    report_exists = check_reports(invoice, supplier)
                    
                    if report_exists:
                        print "Report already exists for invoice " + invoice['invref']
                    else:
                        print "Creating report for invoice " + invoice['invref']
                        
                        #Can't get a list of SIMs from Aeris so have to get the invoice first
                        #and join on this. Different to WL and Eseye

                        #This function returns the invoice, cleaned of stuff we don't want/need
                        AERIS_INVOICE_DF = process_aeris_invoice(supplier, invoice)

                        #Add the entities
                        AERIS_ENTITIES = join_invoice_to_product_entity(supplier, AERIS_INVOICE_DF)
                        
                        #Create the report
                        report_df = create_aeris_report(supplier, invoice, AERIS_ENTITIES)

                        #Create the grouped report
                        create_grouped_report(invoice, supplier, 'GBP')

            if supplier == 'Eseye':

                #Get a list of all provisioned SIMs
                sim_file = "eseye_sims.csv"

                if os.path.isfile(sim_file):
                    update_sim_list = raw_input("Do you want to update the SIM list (Y)? (Warning: will take ages): ")
                else:
                    update_sim_list = "Y"

                if update_sim_list == "Y" or update_sim_list == "y":

                    #Get complete list of Eseye SIMS
                    ESEYE_SIMS = get_eseye_sims()

                try:
                    ESEYE_SIMS
                except NameError:
                    print "Reading SIM list from file..."
                    ESEYE_SIMS = pd.read_csv(sim_file, index_col=0)

                #rename the iccid column
                ESEYE_SIMS.rename(columns = {'ICCID': 'iccid'}, inplace=True)
                
                #All we really want from ESEYE_SIMS is the provisioned status
                #Everything else we can get from the invoice.
                #So, just keep that.
                ESEYE_SIMS = ESEYE_SIMS[['iccid','status']]

                print ESEYE_SIMS.head(20)


                # Run through all invoices and see if a report has been made
                # If not, make one
             
                for invoice in invoice_list:

                    print "Invoice number: " + invoice['invref']

                    # Check if report already exists 
                    report_exists = check_reports(invoice, supplier)
                    
                    if report_exists:
                        print "Report already exists for invoice " + invoice['invref']
                    else:
                        print "Creating report for invoice " + invoice['invref']
                        
                        # Attach the entities
                        ESEYE_SIMS_PLUS_ENTITIES = join_invoice_to_product_entity(supplier, ESEYE_SIMS)
                        
                        #Create the report
                        report_df = create_eseye_report(supplier, invoice, ESEYE_SIMS_PLUS_ENTITIES)

                        #Create the grouped report
                        create_grouped_report(invoice, supplier, 'USD')