import requests
from requests.auth import HTTPBasicAuth
import numpy as np, pandas as pd
import json
import os
import datetime
import base64
import time

from mixpanel import Mixpanel

MIXPANEL_PROJECT_TOKEN = 'afc05365d30e0d56341dbc6bfea123ac'
MIXPANEL_SECRET_KEY= '5f0f2335c112a28b1cd9c65b09d50aac'
mp = Mixpanel(MIXPANEL_PROJECT_TOKEN)

# Input: Leanplum user attribute to search for using Leanplum's exportUser's API call
# Output: response object from Leanplum's export API with export files (key in response object is 'files')
# Sample Output: {"success": true, "files":
# [ "https://leanplum_pipeline.storage.googleapis.com/export_users_3432342342323.csv"], "state": "FINISHED"}
def get_current_leanplum_users_with_attribute(user_attribute):
    get_leanplum_users_in_mixpanel_cohort = requests.Request('POST', 'https://www.leanplum.com/api?action=exportUsers',
    data={'appId':'app_rKwNADoDN5YDPalKRjcaIbLv22iD2DKLG5fXgtab7fc',
    'clientKey':'exp_0eblAdeJhqlVTaYHugH5Rau1lpSxeKchgOcObF9khyM', 'apiVersion':'1.0.6',
    'userAttribute': user_attribute})

    prepared = get_leanplum_users_in_mixpanel_cohort.prepare()
    pretty_print_POST(prepared)
    s = requests.Session()
    resp = s.send(prepared)
    print(resp.status_code)
    print(resp.content)

    json_response = json.loads(resp.content)['response'][0]

    if not resp.status_code == 200 or not json_response["success"]:
        print "request wasn't successful for exportUsers: ", resp.status_code, resp.content
        raise ValueError("request wasn't successful for exportUsers")
    
    job_id = json_response["jobId"]

    get_export_results_response = get_leanplum_export_results(job_id)
    return get_export_results_response

# Input: Leanplum job ID
# Output: response object from Leanplum's exportResult with export files (key in response object is 'files')
# Sample Output: {"success": true, "files":
# [ "https://leanplum_pipeline.storage.googleapis.com/export_users_3432342342323.csv"], "state": "FINISHED"}
def get_leanplum_export_results(job_id):
    get_export_results_response = None

    while not get_export_results_response:
        time.sleep(5)

        get_export_results_request  = requests.Request('POST', 'https://www.leanplum.com/api?action=getExportResults',
        data={'appId':'app_rKwNADoDN5YDPalKRjcaIbLv22iD2DKLG5fXgtab7fc',
        'clientKey':'exp_0eblAdeJhqlVTaYHugH5Rau1lpSxeKchgOcObF9khyM', 'apiVersion':'1.0.6', 'jobId':job_id})


        prepared = get_export_results_request.prepare()
        pretty_print_POST(prepared)
        s = requests.Session()
        resp = s.send(prepared)
        print(resp.status_code)
        print(resp.content)

        export_results_json_response = json.loads(resp.content)['response'][0]

        if resp.status_code == 200 and export_results_json_response['state'] == "FINISHED":
            get_export_results_response = export_results_json_response
            
    print "broke out of while loop"
    return get_export_results_response

def upload_mixpanel_cohorts_to_leanplum():
    cohort_ids = get_cohort_ids_from_file()
    for cohort_id in cohort_ids:
        
        cohort_id = int(cohort_id)

        # get current mixpanel cohort
        # TODO: haven't handled case if this request returns more than 1 page (or 1000) users
        r = requests.get("https://mixpanel.com/api/2.0/engage?filter_by_cohort={\"id\":%d}" % cohort_id,
        auth=HTTPBasicAuth(MIXPANEL_SECRET_KEY, ''))
        print r.content

        df = pd.DataFrame(columns=['mixpanel_distinct_id', 'leanplum_user_id', 'leanplum_cohort_attribute'])
    
        # transform mixpanel response for cohort into dataframe df
        mixpanel_json_http_response = json.loads(r.content)

        if 'error' in mixpanel_json_http_response:
            print "Error: ", mixpanel_json_http_response['error']
            continue

        mixpanel_results = mixpanel_json_http_response['results']
        for i in range(len(mixpanel_results)):
            df.loc[i] = [mixpanel_results[i]["$distinct_id"], mixpanel_results[i]["$properties"]["Name"], "mixpanel_cohort_%d" % (cohort_id) ]
    
        print "df", df

        # get current leanplum cohort (previous_df) and then create two dataframes (one for data to remove and one to
        # add) to update leanplum user attributes to current mixpanel cohort state
        try:
            previous_df = get_current_leanplum_cohort(cohort_id)
        except ValueError:
            continue

        overlap_df = previous_df.loc[previous_df['mixpanel_distinct_id'].isin(df['mixpanel_distinct_id'])]
        
        removed_from_cohort_df = previous_df.set_index('mixpanel_distinct_id').drop(overlap_df['mixpanel_distinct_id']).reset_index()
        add_to_cohort_df = df.set_index('mixpanel_distinct_id').drop(overlap_df['mixpanel_distinct_id']).reset_index()

        add_to_cohort_df['add_or_remove_from_leanplum_value'] = True
        removed_from_cohort_df['add_or_remove_from_leanplum_value'] = None
    
        # final_df is changes made to leanplum user attributes
        final_df = pd.concat([add_to_cohort_df,removed_from_cohort_df])
    
        print "final_df", final_df
    			
        for index, row in final_df.iterrows():
            mixpanel_distinct_id, leanplum_user_id, leanplum_attribute_name, add_or_remove_from_leanplum = row[0], row[1], row[2], row[3]
    
            set_request = requests.Request('POST', 'https://www.leanplum.com/api?action=setUserAttributes',
            data={'appId':'app_rKwNADoDN5YDPalKRjcaIbLv22iD2DKLG5fXgtab7fc',
            'clientKey':'prod_o10RUtXEZ8fvaTNldqDQQrzJpmxgO2YMXBvAgoVF10U', 'apiVersion':'1.0.6', 'userId':leanplum_user_id,
            'userAttributes':json.dumps({leanplum_attribute_name:add_or_remove_from_leanplum,
            'mixpanel_distinct_id':mixpanel_distinct_id})})
            prepared = set_request.prepare()
            pretty_print_POST(prepared)
            s = requests.Session()
            resp = s.send(prepared)
            print(resp.status_code)
            print(resp.content)
    
# Output: array of mixpanel cohort IDs from the cohort file
def get_cohort_ids_from_file():
    lines = None
    with open('cohorts') as f:
        lines = [line.rstrip('\n') for line in f]
    print "lines", lines
    return lines 

# Input: requests.Request object
# Output: nothing. Just prints the requests.Request object into a good format
def pretty_print_POST(req):
    """
    At this point it is completely built and ready
    to be fired; it is "prepared".

    However pay attention at the formatting used in 
    this function because it is programmed to be pretty 
    printed and may differ from the actual request.
    """
    print('{}\n{}\r\n{}\r\n\r\n{}'.format(
        '-----------START-----------',
        req.method + ' ' + req.url,
        '\r\n'.join('{}: {}'.format(k, v) for k, v in req.headers.items()),
        req.body,
    ))

epoch = datetime.datetime.utcfromtimestamp(0)

def unix_time_seconds(dt):
    return (dt - epoch).total_seconds()

def upload_leanplum_events_to_mixpanel():
    
    start_date_time = datetime.datetime.now() - datetime.timedelta(hours=2)

    start_date_input = start_date_time.strftime('%Y%m%d')
    start_time_input = int(unix_time_seconds(start_date_time))

    get_leanplum_data_request = requests.Request('POST', 'https://www.leanplum.com/api?action=exportData',
    data={'appId':'app_rKwNADoDN5YDPalKRjcaIbLv22iD2DKLG5fXgtab7fc',
    'clientKey':'exp_0eblAdeJhqlVTaYHugH5Rau1lpSxeKchgOcObF9khyM', 'apiVersion':'1.0.6',
    'startDate':start_date_input, 'startTime':start_time_input})

    prepared = get_leanplum_data_request.prepare()
    pretty_print_POST(prepared)
    s = requests.Session()
    resp = s.send(prepared)
    print(resp.status_code)
    print(resp.content)

    json_response = json.loads(resp.content)['response'][0]

    if not resp.status_code == 200 or not json_response["success"]:
        print "request wasn't successful for upload_leanplum_events_to_mixpanel"
        return
    
    job_id = json_response["jobId"]

    # mock API call
    #job_id = 'export_5435836278898688_8549284845904944437'

    get_export_results_response = get_leanplum_export_results(job_id)

    response_file_urls = get_export_results_response["files"]

    for response_file_url in response_file_urls:
        response  = requests.get(response_file_url)

        if not response.status_code == 200:
            print "getting file data didn't return 200, instead: ", response.status_code, response.content
            return

        response_in_lines = response.content.split('\n')
        for line in response_in_lines:
            try:
                json_line = json.loads(line)
            except ValueError:
                print "json didn't decode", line
                json_line = ''

            if "userAttributes" in json_line and "mixpanel_distinct_id" in json_line["userAttributes"]:
                mixpanel_distinct_id = json_line["userAttributes"]["mixpanel_distinct_id"]

                events = json_line["states"][0]["events"]

                for event in events:
                    event_time = event["time"]
                    event_id = str(event["eventId"])
                    event_name = "[Leanplum Event] %s" % (event["name"])

                    mp.track(mixpanel_distinct_id, event_name, {
                        '$time': event_time, "$insert_id": event_id})

                    data = { "event": "[Leanplum Event] %s" % (event["name"]), 
                    "properties": {
                        "time": int(event["time"]), 
                        "distinct_id": mixpanel_distinct_id,
                        "$insert_id": str(event["eventId"])
                        }
                    }
                    print 'mixpanel was tracked with data: ', data

# Input: mixpanel cohort ID
# Output: dataframe with columns: leanplum_user_id, mixpanel_distinct_id, leanplum_cohort_attribute
#
# NOTE: this function calls Leanplum's exportUsers API twice, which currently has a limit of 40 API calls per day, which
# means you could only sync every 2 hours data from Mixpanel to Leanplum

def get_current_leanplum_cohort(mixpanel_cohort_id):
    user_attribute = "mixpanel_cohort_%s" % mixpanel_cohort_id
    leanplum_users_in_mixpanel_cohort_api_response = get_current_leanplum_users_with_attribute(user_attribute)
    leanplum_users_with_mixpanel_distinct_id_api_response = get_current_leanplum_users_with_attribute('mixpanel_distinct_id')

    # mock out API call
    #leanplum_users_in_mixpanel_cohort_api_response = {'files': ['https://leanplum_pipeline.storage.googleapis.com/export_users_5435836278898688_2526997884523820047.csv'], 'fields': ['userId'], 'state': 'FINISHED', 'success': True}
    #leanplum_users_with_mixpanel_distinct_id_api_response = {'files': ['https://leanplum_pipeline.storage.googleapis.com/export_users_5435836278898688_2549117437956872536.csv'], 'fields': ['userId'], 'state': 'FINISHED', 'success': True} 

    print leanplum_users_in_mixpanel_cohort_api_response 
    print leanplum_users_with_mixpanel_distinct_id_api_response 

    leanplum_users_in_mixpanel_cohort_df = pd.DataFrame(columns=['leanplum_user_id', 'in_mixpanel_cohort'])
    for csv_file in leanplum_users_in_mixpanel_cohort_api_response['files']:
        input_df = pd.read_csv(csv_file, header=None)
        input_df.columns = ['leanplum_user_id', 'in_mixpanel_cohort'] 
    
        leanplum_users_in_mixpanel_cohort_df = pd.concat([leanplum_users_in_mixpanel_cohort_df,input_df])
        
    leanplum_user_id_and_mixpanel_distinct_id_df = pd.DataFrame(columns=['leanplum_user_id', 'mixpanel_distinct_id'])
    for csv_file in leanplum_users_with_mixpanel_distinct_id_api_response['files']:
        input_df = pd.read_csv(csv_file, header=None)
        input_df.columns = ['leanplum_user_id', 'mixpanel_distinct_id'] 
    
        leanplum_user_id_and_mixpanel_distinct_id_df = pd.concat([leanplum_user_id_and_mixpanel_distinct_id_df,input_df])
    
    print 'first_df', leanplum_user_id_and_mixpanel_distinct_id_df 
    print 'second_df', leanplum_users_in_mixpanel_cohort_df 
    intermediary_merged_df = pd.merge(leanplum_user_id_and_mixpanel_distinct_id_df, leanplum_users_in_mixpanel_cohort_df, on='leanplum_user_id')
    intermediary_merged_df['leanplum_cohort_attribute'] = user_attribute

    print 'intermediary_df', intermediary_merged_df 
    
    current_leanplum_cohort_df = intermediary_merged_df[['leanplum_user_id', 'mixpanel_distinct_id',
    'leanplum_cohort_attribute']]

    return current_leanplum_cohort_df


#upload_leanplum_events_to_mixpanel()
upload_mixpanel_cohorts_to_leanplum()
