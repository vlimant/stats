import httplib,urllib2
import os
import json 
import pprint
from collections import defaultdict
from couchDB import Interface
import traceback
import time
import sys
import copy
from growth import plotGrowth

sys.path.append('/afs/cern.ch/cms/PPD/PdmV/tools/McM/')
from rest import restful

class X509CertAuth(httplib.HTTPSConnection):
    def __init__(self, host, *args, **kwargs):
        x509_path = os.getenv("X509_USER_PROXY", None)
        key_file = cert_file = x509_path
        httplib.HTTPSConnection.__init__(self,  host,key_file = key_file,cert_file = cert_file,**kwargs)

class X509CertOpen(urllib2.AbstractHTTPHandler):
    def default_open(self, req):
        return self.do_open(X509CertAuth, req)

def generic_get(url,header=None, load=True):
    opener=urllib2.build_opener(X509CertOpen())
    datareq = urllib2.Request(url)
    if header:
        for (k,v) in header.items():
            datareq.add_header(k,v)
    #datareq.add_header('authenticated_wget', "The ultimate wgetter")                                                                  
    #   print "Getting material from %s..." %url,                                                                                          
    requests_list_str=opener.open(datareq).read()

    if load:
        return json.loads(requests_list_str)
    else:
        return requests_list_str
def generic_tget(url,header=None):
    return generic_get(url,header,load=False)

def crabStatus( task_name ):
    status = generic_get('https://cmsweb.cern.ch/crabserver/prod/workflow/?workflow=%s'% task_name ,header={"User-agent":"CRABClient/3.3.9","Accept": "*/*"})
    if not 'result' in status: print "Cannot find result for",task_name
    status=status['result']
    if len(status)!=1: print "Wrong result for",task_name
    status=status[0]
    return status


def fullStatus( task_name ):
    statusd = crabStatus( task_name )
    out=defaultdict(int)
    if 'outdatasets' in statusd:
        output_datasets = statusd['outdatasets']
        dbs3_url='https://cmsweb.cern.ch/dbs/prod/phys03/DBSReader/'
        for ds in output_datasets:
            urlds="%s/filesummaries?dataset=%s"%( dbs3_url, ds)
            ret = generic_get(urlds)
            for r in ret: out[ds]+=r['num_event']
    #pprint.pprint(dict(out))
    status = statusd['status']
    return status, dict(out)

def updateStats( task_name , rid=None):
    ## get the status from crab3
    status, outs = fullStatus( task_name )

    ds_status='PRODUCTION'
    if status=='COMPLETED': status,ds_status='announced','VALID'

    ## get the doc from stats : the doc has the name of the crab3 task
    statsCouch = Interface('http://cms-pdmv-stats.cern.ch:5984/stats')
    try:
        statsDoc = statsCouch.get_file_info( task_name )
    except urllib2.HTTPError as e:
        print "could not get stats doc for",task_name
        if e.code==404:
            statsDoc=None
        else:
            return 

    if not statsDoc:
        print "Adding a fresh doc for",task_name
        if not rid:
            print "this is not possible to insert a doc without knowing the request prepid"
            return
        ## if not in stats already : add from scratch
        mcm=restful(dev=False)
        mcm_r = mcm.getA('requests', rid )

        statsDoc = {"_id": task_name,
                    "pdmv_expected_events":0,
                    "pdmv_open_evts_in_DAS":0,
                    "pdmv_evts_in_DAS":0,
                    "pdmv_status_from_reqmngr":status,
                    "pdmv_prep_id": rid,
                    "pdmv_dataset_name":"",
                    "pdmv_dataset_list":[],
                    "pdmv_request_name": task_name,
                    "pdmv_status_in_DAS":ds_status,
                    }

        
        additional = {
            "pdmv_input_dataset":mcm_r['input_dataset'],
            "pdmv_expected_events": mcm_r['total_events'],
            "pdmv_campaign": mcm_r['member_of_campaign'],
            "pdmv_configs": mcm_r['config_id'],

            "pdmv_submission_date": task_name.split("_")[0],
            "pdmv_submission_time": task_name.split("_")[1],

            "pdmv_completion_in_DAS": 0, #define it as the fraction of 
            "pdmv_running_days":0,##define it from time
            "pdmv_completion_eta_in_DAS": -1,#define it from fraction and time
            "pdmv_type":"Crab3", # to classify the docs
            ## needed to show up in stats
            "pdmv_priority" : mcm_r['priority'],
            "pdmv_running_jobs": 0,
            "pdmv_pending_jobs" : 0,
            "pdmv_all_jobs": 0,
            "pdmv_completion_eta_in_DAS": -1,
            "pdmv_status" : "N/A",
            "pdmv_request" : {},
            "pdmv_present_priority" : mcm_r['priority'],
            "pdmv_completion_in_DAS" : 0,
            "pdmv_monitor_time" : time.asctime(),
            "pdmv_monitor_history":[],
            }
        statsDoc.update( additional )

        print "adding doc for",task_name
        statsCouch.create_file( json.dumps(statsDoc) )
        print "getting it back"
        statsDoc = statsCouch.get_file_info( task_name )

    ## update with only what is necessary
    if len( outs ):
        pprint.pprint( outs )
        content={'pdmv_evts_in_DAS' : min(outs.values()),
                 "pdmv_status_from_reqmngr":status,
                 "pdmv_status_in_DAS":ds_status,
                 "pdmv_dataset_list" : outs.keys(),
                 "pdmv_dataset_name" : outs.keys()[0],
                 "pdmv_dataset_statuses" : {}, 
                 "pdmv_completion_in_DAS" : float( min(outs.values()) / statsDoc["pdmv_expected_events"]),
                 "pdmv_monitor_time" : time.asctime(),
                 }
        for o in outs: ## this is really a mandatory information
            content["pdmv_dataset_statuses"][o]={"pdmv_open_evts_in_DAS":0,
                                                 "pdmv_evts_in_DAS" : outs[o],
                                                 "pdmv_status_in_DAS":ds_status}
        
        statsDoc.update( content )
        ## creating history entry
        to_get=['pdmv_monitor_time','pdmv_evts_in_DAS','pdmv_open_evts_in_DAS','pdmv_dataset_statuses']
        rev = {}
        for g in to_get:
            if not g in statsDoc: continue
            rev[g] = copy.deepcopy(statsDoc[g])
        statsDoc['pdmv_monitor_history'].insert(0, rev)

        ## making the growth plot
        print "making the growth plot"
        plotGrowth(statsDoc,None)
        ## pushing back to the db
        print "pushing",statsDoc,"back to stats"
        statsCouch.update_file( task_name, json.dumps(statsDoc) )
    else:
        print status 
        pprint.pprint( outs )

def updateCycle():
    mcm=restful(dev=False)
    rs = mcm.getA('requests',query='status=submitted&private=true')
    for r in rs:
        task_name = r['reqmgr_name'][0]['name']
        rid = r['prepid']
        updateStats( task_name, rid )

if __name__ == '__main__':
    #status = crabStatus('140909_085025_crab3test-1:vlimant_crab_prod_HIG-Summer12-02245-v10')
    #pprint.pprint(status)

    #status, out = fullStatus('140909_085025_crab3test-1:vlimant_crab_prod_HIG-Summer12-02245-v10')  
    #status, out = fullStatus('140822_094615_crab3test-4:vlimant_crab_prod_HIG-Summer12-02245_v4')
    #print status
    #pprint.pprint(out)

    #updateStats('140822_094615_crab3test-4:vlimant_crab_prod_HIG-Summer12-02245_v4')
    updateStats('140909_085025_crab3test-1:vlimant_crab_prod_HIG-Summer12-02245-v10','HIG-Summer12-02245') 
    #updateStats('140814_140806_crab3test-1:vlimant_crab_prod_HIG-Summer12-02245')

    
