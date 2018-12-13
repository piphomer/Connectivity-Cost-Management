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
import datetime

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

def make_product_entity_state_df(month_start, month_end):

    #Join pel_df and entity_df
    product_entity_state_df = pel_df.merge(entities_df, on='entity_id', how="left")

    #Make date_added a datetime object
    product_entity_state_df.date_added = pd.to_datetime(product_entity_state_df.date_added)

    #Put future date in any null date_removed fields
    product_entity_state_df.date_removed = product_entity_state_df.date_removed.fillna(value = dt.strptime('2100-12-31', "%Y-%m-%d"))
    product_entity_state_df.date_removed = pd.to_datetime(product_entity_state_df.date_removed) #Make sure it's datetime

    #only keep entity linkers that were created before or during month of interest
    product_entity_state_df = product_entity_state_df[product_entity_state_df.date_added < month_end]

    #Only keep entity linkers that were not removed by end of month of interest
    product_entity_state_df = product_entity_state_df[product_entity_state_df.date_removed > month_end]
 
    #Make the IMEI a string
    product_entity_state_df.product_imei.astype('str')

    #Change the index to IMEI so we can drop duplicate rows more easily
    product_entity_state_df.set_index('product_imei', inplace=True)

    print "Looking for IMEIs with multiple entities..."

    for imei, group in product_entity_state_df.groupby('product_imei'):

        if group.shape[0] > 1: #only look at imei's with more than one current entity

            #print imei

            #First, remove this IMEI from the dataframe entirely. We will then add back in only the row we deem relevant
            product_entity_state_df = product_entity_state_df.drop(imei)

            if group.shape[0] > 1:
                group = group[group['entity'] != 'Unknown Entity'] #remove rows that are Unknown Entity
            if group.shape[0] > 1:
                group = group[group['entity'] != 'Orange Energy All'] #remove rows that are Orange Energy All
            if group.shape[0] > 1:
                group = group[group['entity'] != 'Aceleron'] #remove rows that are Aceleron
            if group.shape[0] > 1:
                group = group[group['entity'] != 'BBOXX Engineering'] #remove rows that are BBOXX Engineering
            if group.shape[0] > 1:
                group = group[group['entity'] != 'BBOXX Asia'] #remove rows that are BBOXX Asia 
            
            if group.shape[0] > 1: #if there is STILL more than one IMEI on this entity...
                group['date_added_copy'] = pd.to_datetime(group['date_added']) #add a column with a pandas datetime or it won't sort properly
                group.sort_values(by='date_added_copy', ascending=False, inplace=True)
                group = group.iloc[0] #just take the first (newest) one
                group.drop(['date_added_copy'], inplace=True) #drop that added column

            product_entity_state_df = product_entity_state_df.append(group) #Append the resulting one-row group back into the dataframe

    print "One-to-one IMEI:entity mapping complete! There are now ", product_entity_state_df.shape[0], "products in the list."

    print product_entity_state_df.head(5)

    print "Merging with the Products table"
    #Join the Product table so we can get the IMSI and the ICCID
    product_entity_state_df = product_entity_state_df.merge(products_df[['imsi','iccid','product_imei']], left_index=True, right_on='product_imei')



    #Discard any states that happened after the month of interest
    states_filtered_df = states_df[states_df.created_at < month_end]

    print "Length of states table after removing states created after this month: ", states_filtered_df.shape[0]

    # print states_filtered_df.head(10)

    #Only keep the latest state for each product, that should be the state at month end.
    
    print "Finding latest state for this month."

    #Just save the most recent state    
    states_filtered_df = states_filtered_df.sort_values('state_id',ascending=False).drop_duplicates('product_imei').reset_index(drop=True).set_index('product_imei')

    print "Done... already!!!"

    product_entity_state_df = product_entity_state_df.sort_values('product_imei')
    states_filtered_df = states_filtered_df.sort_index()

    product_entity_state_df = product_entity_state_df.merge(states_filtered_df, how='left', left_on='product_imei', right_index=True)

    print product_entity_state_df.head(7)

    print "product_entity_state_df size after merge: {}".format(product_entity_state_df.shape[0])

    product_entity_state_df.set_index('product_imei', inplace=True)

    product_entity_state_df = product_entity_state_df.reset_index().merge(state_types_df, \
                                    how = 'left', left_on = 'current_state_type', right_on = 'state_type_id').set_index('product_imei')

    print "product_entity_state_df looks like:"
    print product_entity_state_df.head(5)

    return product_entity_state_df

########################################################################

def make_invoice_path(supplier):
    invoice_path = './invoices/{}/csv/'.format(supplier)
    return invoice_path

def make_invoice_filename(invoice,supplier):
    #invoice_filename = invoice['date'] + "_" + invoice['invref'] + ".csv"

    invdate = dt.date(dt.strptime(invoice['date'], '%Y%m%d')).replace(day = 1)
    invdate = invdate - datetime.timedelta(days = 1)

    invoice_filename = "{}_invoice_{}.csv".format(dt.strftime(invdate,'%Y%m'),supplier)

    return invoice_filename

########################################################################

def make_report_filename(month, supplier):

    # print invoice
    # #Create a datetime object from the date value of the invoice
    # try:
    #     dateobject = dt.strptime(invoice['date'], "%Y%m")
    # except:
    #     dateobject = dt.strptime(invoice['date'], "%Y%m%d")
    

    #Generate a nicer date string to name the report by
    report_filename = dt.strftime(month, "%Y-%m (%b)") + " " + supplier + " Cost Report.csv"

    print report_filename
    
    return report_filename

########################################################################

def make_grouped_report_filename(month, supplier):
       
    #Generate a nicer date string to name the report by
    grouped_report_filename = "./CCM Reports/" + dt.strftime(month, "%Y-%m (%b)") + " - {} - CCM Report.csv".format(supplier)

    return grouped_report_filename

########################################################################

def download_new_invoices(supplier):

    if supplier == 'WL' or supplier == 'Intelligent':
        
        #Get list of all available invoices
        if supplier == 'WL':
            URL = WIRELESS_LOGIC_API_BASE + "invoices?_format=json&billing-account=" + BBOXX_ACCOUNT_WL
        else:
            URL = WIRELESS_LOGIC_API_BASE + "invoices?_format=json&billing-account=" + BBOXX_ACCOUNT_INTELLIGENT

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

            invref = invoice['invref'].replace("/","-")
            invdate = invoice['date'].replace("-","")

            invdict = {'invref': invref, 'date' : invdate}
            invoice_list.append(invdict)

        #Loop through the list of invoices, check if they are already downloaded, if not download them

        for invoice in invoice_list:

            invoice_filename = make_invoice_filename(invoice, supplier)
            invoice_path = make_invoice_path(supplier)
          
       
            #Check if invoice already in the folder
            if os.path.isfile(invoice_path+invoice_filename):
                print "Invoice " + invoice['invref'] + " already downloaded." 
            
            else: #Otherwise, download it

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


    if supplier == 'Aeris' or supplier == 'Eseye':
        print "{} invoices have to be downloaded manually for now.".format(supplier)

    return

####################################################################################################

def check_report_exists(month, supplier):

    grouped_report_filename = make_grouped_report_filename(month, supplier)

    #Check if report already exists
    if os.path.isfile(grouped_report_filename):
        return True
    else:
        return False


def create_grouped_report(month, supplier, currency = 'GBP'):

    grouped_report_filename = make_grouped_report_filename(month, supplier)

    #Group the results by entity
    grouped_df = report_df[['entity','state','tariff','rental','data','sms','total']]
    print grouped_df.head(50)
    # print grouped_df['total'].sum()

    grouped_df = grouped_df.groupby(['entity','state','tariff'], \
                    axis = 0).agg({'rental': 'sum','data': 'sum','sms': 'sum','total' : ['sum', 'count']})

    #Flatten the column indexes
    grouped_df = grouped_df.reset_index()
    grouped_df.columns = [' '.join(col).strip() for col in grouped_df.columns.values]

    #Rearrange the columns
    grouped_df = grouped_df[['entity','state','tariff','total count','rental sum','sms sum','data sum','total sum']]

    #Rename the columns
    grouped_df = grouped_df.rename(index=str, columns={'total count': 'count',
                                                        'rental sum': 'rental',
                                                        'sms sum': 'sms',
                                                        'data sum': 'data',
                                                        'total sum': 'total'})

    #Add a column with the invoice month to filter on in Power BI
    report_month = dt.strftime(month, "%m-%Y")

    
    grouped_df['month'] = report_month

    #Add a column showing the supplier
    grouped_df['supplier'] = supplier

    #Add a column showing that WL costs are in USD
    grouped_df['currency'] = currency
    
    #Set the index column
    grouped_df.set_index('entity', inplace=True)

    #Rearrange the columns of the grouped report
    grouped_df = grouped_df[['supplier','state','tariff','count','rental','sms','data','total','month','currency']]

    #Output the file
    print "Generating report for invoice " + report_month
    grouped_df.to_csv(grouped_report_filename)
    
####################################################################################################

def create_wl_report(supplier, month):

    invoice_filename = "{}_invoice_{}.csv".format(dt.strftime(month, '%Y%m'),supplier)
    invoice_path = make_invoice_path(supplier)

    report_filename = make_report_filename(month, supplier)
    report_path = './Full Reports/{}/'.format(supplier)

    #Read the invoice file
    try:
        report_df = pd.read_csv(invoice_path + invoice_filename, dtype={'imsi': str, 'iccid': str})
    except:
        print "Invoice {} does not seem to exist! Skipping...".format(invoice_filename)
        return

    #Just keep the columns we're interested in
    report_df = report_df[['ctn','rental','gprs','gprs_usage','gprsroam',
                            'sms','sms_usage','smsroam','nettotal']]

    #Merge the tariffs
    report_df = report_df.merge(sim_list_df_dict[supplier], on='ctn', how='left')


    #Sum the gprs + gprs_roam values and sms + sms_roam values
    report_df['gprs'] = report_df['gprs'] + report_df['gprsroam']
    report_df['sms'] = report_df['sms'] + report_df['smsroam']


    #Merge with product_entity_state_df
    #Do it on ICCID if Intelligent, IMSI if not
    if supplier == 'WL':
        report_df = report_df.merge(product_entity_state_df, on='imsi', how='left')
    else:
        report_df = report_df.merge(product_entity_state_df, on='iccid', how='left')

    print report_df.head(3)


    #If entity field is blank, product must have been deleted from Smart Solar
    report_df = report_df.fillna(value = {'entity_id': 0, 'entity': 'Product not in SmartSolar', 'state': 'N/A'})

    print report_df.head(3)

    #Just pull the fields we are interested in
    report_df = report_df.drop(['date_added','date_removed','id','msisdn','workflow_status','smsroam','gprsroam'], axis=1)



    #Rename a couple of columns
    report_df = report_df.rename(index=str, columns={'nettotal': 'total',
                                                        'name': 'entity',
                                                        'gprs': 'data',
                                                        'gprs_usage': 'data_usage',
                                                        'tariff_name': 'tariff'})
    
    #Itemised report
    report_df.to_csv(report_path+report_filename)

    return report_df

####################################################################################################

def update_sim_list(supplier):

    if supplier == 'WL':

        wl_sims_file = "WL_sims_plus_tariffs.csv" #this file contains all WL SIMs, including Intelligent ones

        #Get complete list of SIMs + tariffs they are on
        if os.path.isfile(wl_sims_file):
            update_wl_sims = raw_input("Do you want to update the WL SIM list (Y)? (Warning: will take several hours to run): ")
        else:
            update_wl_sims = "Y"

        if update_wl_sims == "Y" or update_wl_sims == "y":

            #Get complete list of WL SIMs including the Intelligent SIMs
            print "Getting list of WL SIMs, including Intelligent"

            URL = WIRELESS_LOGIC_API_BASE + "sims?_format=json&billing-account=" + BBOXX_ACCOUNT_WL
            
            r = requests.get(url=URL, auth=AUTH)

            r_json = r.json()['sims']

            #Make a dataframe with all (WL and Intelligent) SIMs
            wl_sims_df = make_df(r_json)

            print "Number of WL + Intelligent SIMs: {}".format(len(wl_sims_df))

            wl_sims_df.to_csv('WL_sims.csv')

            #Get tariff details
            #wl_sims_plus_tariffs_df = get_df_of_wl_tariffs()

            #create a list from the wl_sims_df
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

            #Add the CTN
            print "Adding CTNs to dataframe..."

            def add_ctn(wl_sims_plus_tariffs_df):
                if wl_sims_plus_tariffs_df['mno_account'] == 'Vodafone Global':
                    wl_sims_plus_tariffs_df['ctn'] = wl_sims_plus_tariffs_df['imsi']
                else:
                    wl_sims_plus_tariffs_df['ctn'] = wl_sims_plus_tariffs_df['msisdn']

                return wl_sims_plus_tariffs_df

            # Run the .apply function over the dataframe
            wl_sims_plus_tariffs_df = wl_sims_plus_tariffs_df.apply(add_ctn,axis=1)

            #wl_sims_plus_tariffs_df.set_index('iccid', inplace=True)

            #Write it to the dict of SIM lists
            sim_list_df_dict[supplier] = wl_sims_plus_tariffs_df


            #Resulting dataframe contains account details for ALL WL SIMs including Intelligent
            print "Writing csv file..."
            wl_sims_plus_tariffs_df.to_csv(wl_sims_file)
            print "Success!"

        else:
            sim_list_df_dict[supplier] = pd.read_csv(wl_sims_file, dtype={'imsi': str, 'iccid': str})

    elif supplier == 'Intelligent':
        #We only need to get wl_sim_list once so we'll do that on WL and do nothing here
        sim_list_df_dict[supplier] = sim_list_df_dict['WL']

    elif supplier == 'Aeris':

        pass

    elif supplier == 'Eseye':

        eseye_sims_file = "eseye_sims.csv"

        if os.path.isfile(eseye_sims_file):
            update_eseye_sims = raw_input("Do you want to update the Eseye SIM list (Y)? (Warning: will take several hours to run): ")
        else:
            update_eseye_sims = "Y"

        if update_eseye_sims == "Y" or update_eseye_sims == "y":

            eseye_sims = [] #A list to hold the Eseye SIMs

            sims_per_page = 10

            while True:

                print len(eseye_sims)

                page = get_eseye_sims_by_page(len(eseye_sims), sims_per_page)
                
                if len(page) == 0:
                    break

                eseye_sims.extend(page)

            eseye_sims_df = make_df(eseye_sims) #Turn the list into a dataframe

            sim_list_df_dict[supplier] = eseye_sims_df

            eseye_sims_df.to_csv('eseye_sims.csv')    

            print "Total SIMs returned: {}".format(len(eseye_sims))

        else:
            sim_list_df_dict[supplier] = pd.read_csv(eseye_sims_file)

    else:
        print "Unidentified SIM supplier!!!"

    return sim_list_df_dict



####################################################################################################
####################################################################################################
####################################################################################################


if __name__ == '__main__':


    #Check for new invoices via the suppliers' APIs
    for supplier in supplier_list:

        print "Looking for new invoices from " + supplier

        download_new_invoices(supplier)

    #Update SIM/tariff lists from each supplier and open them

    sim_list_df_dict = {} #We will store the sim_list_dfs in a dict so we can reference them by supplier

    for supplier in supplier_list:

        sim_list_df_dict = update_sim_list(supplier)



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

        # print product_entity_state_df.head(10)

        #Workflow is different for each SIM provider

        for supplier in supplier_list:

            print "Working on " + supplier

            invoice_path = make_invoice_path(supplier)
            invoice_fname = "{}_invoice_{}.csv".format(dt.strftime(month, '%Y%m'),supplier)

            
            #Get invoice for this month
            if supplier == 'WL' or supplier == 'Intelligent':

                # Check if report already exists 
                report_exists = check_report_exists(month, supplier)
                    
                if report_exists:
                    print "Report already exists for this month"

                else:
                    print "Creating report for this month"

                    if os.path.isfile(os.path.join(invoice_path,invoice_fname)):

                        # print "Opening {} invoice".format(supplier)
                        # invoice_df = pd.read_csv(invoice_path + invoice_fname)

                        #Create the full report
                        report_df = create_wl_report(supplier, month)

                        #Create the grouped report
                        create_grouped_report(month, supplier, 'GBP')

                    else:
                        print "{} invoice does not exist for this month.".format(supplier)







            # if supplier == 'Aeris':
                    
            #     for invoice in invoice_list:

            #         print "Invoice number: " + invoice['invref']

            #         # Check if report already exists 
            #         report_exists = check_reports(invoice, supplier)
                    
            #         if report_exists:
            #             print "Report already exists for invoice " + invoice['invref']
            #         else:
            #             print "Creating report for invoice " + invoice['invref']
                        
            #             #Can't get a list of SIMs from Aeris so have to get the invoice first
            #             #and join on this. Different to WL and Eseye

            #             #This function returns the invoice, cleaned of stuff we don't want/need
            #             AERIS_INVOICE_DF = process_aeris_invoice(supplier, invoice)

            #             #Add the entities
            #             AERIS_ENTITIES = join_invoice_to_product_entity(supplier, AERIS_INVOICE_DF)
                        
            #             #Create the report
            #             report_df = create_aeris_report(supplier, invoice, AERIS_ENTITIES)

            #             #Create the grouped report
            #             create_grouped_report(invoice, supplier, 'GBP')

            # if supplier == 'Eseye':

            #     #Get a list of all provisioned SIMs
            #     sim_file = "eseye_sims.csv"

            #     if os.path.isfile(sim_file):
            #         update_sim_list = raw_input("Do you want to update the SIM list (Y)? (Warning: will take ages): ")
            #     else:
            #         update_sim_list = "Y"

            #     if update_sim_list == "Y" or update_sim_list == "y":

            #         #Get complete list of Eseye SIMS
            #         ESEYE_SIMS = get_eseye_sims()

            #     try:
            #         ESEYE_SIMS
            #     except NameError:
            #         print "Reading SIM list from file..."
            #         ESEYE_SIMS = pd.read_csv(sim_file, index_col=0)

            #     #rename the iccid column
            #     ESEYE_SIMS.rename(columns = {'ICCID': 'iccid'}, inplace=True)
                
            #     #All we really want from ESEYE_SIMS is the provisioned status
            #     #Everything else we can get from the invoice.
            #     #So, just keep that.
            #     ESEYE_SIMS = ESEYE_SIMS[['iccid','status']]

            #     print ESEYE_SIMS.head(20)


            #     # Run through all invoices and see if a report has been made
            #     # If not, make one
             
            #     for invoice in invoice_list:

            #         print "Invoice number: " + invoice['invref']

            #         # Check if report already exists 
            #         report_exists = check_reports(invoice, supplier)
                    
            #         if report_exists:
            #             print "Report already exists for invoice " + invoice['invref']
            #         else:
            #             print "Creating report for invoice " + invoice['invref']
                        
            #             # Attach the entities
            #             ESEYE_SIMS_PLUS_ENTITIES = join_invoice_to_product_entity(supplier, ESEYE_SIMS)
                        
            #             #Create the report
            #             report_df = create_eseye_report(supplier, invoice, ESEYE_SIMS_PLUS_ENTITIES)

            #             #Create the grouped report
            #             create_grouped_report(invoice, supplier, 'USD')