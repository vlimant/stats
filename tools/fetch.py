#! /usr/bin/env python

req2dataset="https://cmsweb.cern.ch/reqmgr/reqMgr/outputDatasetsByRequestName/"
request_detail_address="https://cmsweb.cern.ch/reqmgr/view/showWorkload?requestName="
dbs3_url = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader/'

#################
import sys
import os
import json
import pprint
import datetime
import os
import httplib
import urllib2
import urllib
import time
from threading import Thread
from couchDB import Interface
from phedex import phedex,runningSites,custodials,atT2,atT3
sys.path.append('/afs/cern.ch/cms/PPD/PdmV/tools/McM/')
from rest import restful
import traceback
import copy
from growth import plotGrowth
import optparse
from Performances import Performances
from TransformRequestIntoDict import TransformRequestIntoDict
import traceback
#################
#-------------------------------------------------------------------------------  
# Needed for authentication

class X509CertAuth(httplib.HTTPSConnection):
    '''Class to authenticate via Grid Certificate'''
    def __init__(self, host, *args, **kwargs):
      key_file = None
      cert_file = None
  
      x509_path = os.getenv("X509_USER_PROXY", None)
      if x509_path and os.path.exists(x509_path):
        key_file = cert_file = x509_path
  
      if not key_file:
        x509_path = os.getenv("X509_USER_KEY", None)
        if x509_path and os.path.exists(x509_path):
          key_file = x509_path
  
      if not cert_file:
        x509_path = os.getenv("X509_USER_CERT", None)
        if x509_path and os.path.exists(x509_path):
          cert_file = x509_path
  
      if not key_file:
        x509_path = os.getenv("HOME") + "/.globus/userkey.pem"
        if os.path.exists(x509_path):
          key_file = x509_path
  
      if not cert_file:
        x509_path = os.getenv("HOME") + "/.globus/usercert.pem"
        if os.path.exists(x509_path):
          cert_file = x509_path
  
      if not key_file or not os.path.exists(key_file):
        print >>stderr, "No certificate private key file found"
        exit(1)
  
      if not cert_file or not os.path.exists(cert_file):
        print >>stderr, "No certificate public key file found"
        exit(1)
  
      httplib.HTTPSConnection.__init__(self,  host,key_file = key_file,cert_file = cert_file,**kwargs)

#-------------------------------------------------------------------------------

class X509CertOpen(urllib2.AbstractHTTPHandler):
    def default_open(self, req):
      return self.do_open(X509CertAuth, req)

#-------------------------------------------------------------------------------

def generic_get(url, do_eval=True):
    opener=urllib2.build_opener(X509CertOpen())  
    datareq = urllib2.Request(url)
    datareq.add_header('authenticated_wget', "The ultimate wgetter")  
#   print "Getting material from %s..." %url,
    requests_list_str=opener.open(datareq).read()  

    ret_val=requests_list_str
    #print requests_list_str
    if do_eval:
        ret_val=json.loads(requests_list_str)
    return ret_val
#-------------------------------------------------------------------------------

def generic_post(url, data_input):

    data = json.dumps(data_input)
    opener = urllib2.build_opener(X509CertOpen())
    datareq = urllib2.Request(url, data, {"Content-type" : "application/json"})
    requests_list_str = opener.open(datareq).read()
    ret_val = requests_list_str

    return ret_val
#-------------------------------------------------------------------------------

def get_requests_list(pattern="", not_in_wmstats=False):

    if not_in_wmstats:
      return get_requests_list_old(pattern)
    
    opener=urllib2.build_opener(X509CertOpen())  
    url="https://cmsweb.cern.ch/wmstats/_design/WMStats/_view/requestByStatusAndType?stale=update_after"
    datareq = urllib2.Request(url)
    datareq.add_header('authenticated_wget', "The ultimate wgetter")  
    print "Getting the list of requests from %s..." %url,
    requests_list_str=opener.open(datareq).read()  
    print " Got it in %s Bytes"%len(requests_list_str)
    data = json.loads( requests_list_str )
    ## build backward compatibility
    req_list= map( lambda item : {"request_name" : item[0], "status" : item[1], "type" :item[2]}, map(lambda r : r['key'] , data['rows']))
  
    return req_list

def get_dataset_list(reqname):
  #print "getting dataset name for %s" % reqname
  opener=urllib2.build_opener(X509CertOpen())  
  url="%s%s"%(req2dataset,reqname)  
  datareq = urllib2.Request(url)  
  datareq.add_header('authenticated_wget', "The ultimate wgetter")  
  #print "Asking for dataset %s" %url
  datasets_str=opener.open(datareq).read()  
  dataset_list=json.loads(datasets_str)

  return dataset_list

  
def configsFromWorkload( workload ):
  if not 'request' in workload: return []
  if not 'schema' in workload['request']: return []
  schema = workload['request']['schema']
  res=[]
  if schema['RequestType'] == 'TaskChain':
    i=1
    while True:
      t='Task%s'%i
      if t not in schema: break
      if not 'ConfigCacheID' in schema[t]: break
      res.append(schema[t]['ConfigCacheID'])
      i+=1
    else: pass
  else: pass
    ## could browse for StepOneConfig... and so on

  return res

def get_expected_events_by_output( request_name ):
  try:
    return get_expected_events_by_output_( request_name )
  except:
    print "something went wrong in estimating the expected by output. no breaking the update though"
    print traceback.format_exc()
    return {}

def get_expected_events_by_output_( request_name ):
  def get_item( i, d ):
    if type(d)!=dict: 
      return None
    if not d:
      return None
    if i in d: 
      return d[i]
    else:
      for (k,v) in d.items():
        r=get_item(i, v)
        if r: return r

  def unique( l ):
    rl = []
    for i in l:
      if not i in rl:
        rl.append(i)
    return rl
  
  def get_strings( d ):
    ## retrieve all leaves that are strings
    if type(d)!=dict or not d:
      return None
    ret=[]
    for (k,v) in d.items():
      if type(v) == str:
        ret.append(v)
      else:
        r=get_strings(v)
        if r:
          ret.extend(r)
    return ret

  dict_from_workload_local = getDictFromWorkload( request_name, attributes=['schema','dataTier',['subscriptions','dataset']])
  if not dict_from_workload_local:
    return {}

  schema = dict_from_workload_local['request']['schema']
    
  if schema['RequestType'] != 'TaskChain':
    ## this is the next thing to work on, make it work for non-taskchain  
    return {}

  task_i=1
  task_map = {}
  while task_i:
    k='Task%d'%(task_i)
    task_i+=1
    if k in schema:
      task =schema[k]
      task_map[ task['TaskName']] = k
    else:
      break
    
  expectedEvents='ExpectedNumEvents'
  expectedOuputs='ExpectedOutputs'
  while not all(map( lambda d : expectedEvents in d.keys() and expectedOuputs in d.keys(), [ schema[t] for t in task_map.values()])):
    for (taskname,taski) in task_map.items():
      task = schema[taski]
      if not expectedEvents in task:
        if 'RequestNumEvents' in task:
          #print "from expected",taskname
          task[expectedEvents] = task['RequestNumEvents']
          if 'FilterEfficiency' in task:
            task[expectedEvents] *= task['FilterEfficiency']
        elif 'InputDataset' in task:
          rne=None
          ids = [task['InputDataset']]
          bwl = []
          rwl = []
          f= 1.
          if 'BlockWhitelist' in task:
            bwl = task['BlockWhitelist']
          if 'RunWhitelist' in task:
            rwl = task['RunWhitelist']
          if 'FilterEfficiency' in task:
            f = task['FilterEfficiency']
          task[expectedEvents] = get_expected_events_withinput(rne,ids,bwl,rwl,f)
        else:
          previous=task_map[ task['InputTask'] ]
          if expectedEvents in schema[ previous ]:
            #print "from previous",taskname, previous
            task[expectedEvents] = schema[ task_map[ task['InputTask'] ] ][expectedEvents]
            if 'FilterEfficiency' in task:
              task[expectedEvents] *= task['FilterEfficiency']
      if not expectedOuputs in task:
        what=get_item( taskname, dict_from_workload_local['tasks'] )
        #pprint.pprint( what)
        task[expectedOuputs] = get_strings( what['subscriptions'] )
        task['plenty'] = unique(filter(lambda s : '/' in s, get_strings( what['tree'] )))

  ##pprint.pprint( schema )
  
  expected_per_dataset={}
  for (taskname,taski) in task_map.items():           
    task = schema[taski]          
    for d in task[expectedOuputs]:
      expected_per_dataset[d] = task[expectedEvents]
      
  return expected_per_dataset

def get_expected_events(request_name):
    workload_info=generic_get(request_detail_address+request_name, False)
    def getfield(line):
        line=line.replace("<br/>","").replace('\n','').replace(' ','')
        return line.split("=")[-1]

    filter_eff=1.
    rne=None
    ids=[]
    bwl=[]
    rwl=[]
    for line in workload_info.split('\n'):
        if 'request.schema.RequestNumEvents' in line or 'request.schema.RequestSizeEvents' in line:
            rne=eval(getfield(line)) # int or None
        if 'request.schema.InputDatasets' in line:
            ids=eval(getfield(line)) # list
        if 'request.schema.BlockWhitelist' in line:
            bwl=eval(getfield(line)) # list
        if 'request.schema.FilterEfficiency' in line:
            filter_eff=float(eval(getfield(line))) # float
        if 'request.schema.RunWhitelist' in line:
            rwl=eval(getfield(line))

    return get_expected_events_withinput(float(rne.replace("'","")),ids,bwl,rwl,filter_eff)
  
def get_expected_events_withdict(dict_from_workload):
  if 'RequestNumEvents' in dict_from_workload['request']['schema']:
    rne=dict_from_workload['request']['schema']['RequestNumEvents']
  elif 'RequestSizeEvents' in dict_from_workload['request']['schema']:
    rne=dict_from_workload['request']['schema']['RequestSizeEvents']
  elif 'Task1' in dict_from_workload['request']['schema'] and 'RequestNumEvents' in dict_from_workload['request']['schema']['Task1']:
    rne=dict_from_workload['request']['schema']['Task1']['RequestNumEvents']
  else:
    rne=None
    
  if 'FilterEfficiency' in dict_from_workload['request']['schema']:
    f=float(dict_from_workload['request']['schema']['FilterEfficiency'])
  elif 'Task1' in dict_from_workload['request']['schema'] and 'FilterEfficiency' in dict_from_workload['request']['schema']['Task1']:
    f=float(dict_from_workload['request']['schema']['Task1']['FilterEfficiency'])
  else:
    f=1.
    
  if 'InputDatasets' in dict_from_workload['request']['schema']:
    try:
      ids=dict_from_workload['request']['schema']['InputDatasets'].split(',')
    except:
      ids=dict_from_workload['request']['schema']['InputDatasets']
  elif 'Task1' in dict_from_workload['request']['schema'] and  'InputDataset' in dict_from_workload['request']['schema']['Task1']:
    try:
      ids=dict_from_workload['request']['schema']['Task1']['InputDataset'].split(',')
    except:
      ids=dict_from_workload['request']['schema']['Task1']['InputDataset']
  else:
    ids=[]

    

  if 'BlockWhitelist' in dict_from_workload['request']['schema']:
    try:
      bwl=dict_from_workload['request']['schema']['BlockWhitelist'].split(',')
    except:
      bwl=dict_from_workload['request']['schema']['BlockWhitelist']
  elif 'Task1' in dict_from_workload['request']['schema'] and  'BlockWhitelist' in dict_from_workload['request']['schema']['Task1']:
    try:
      bwl=dict_from_workload['request']['schema']['Task1']['BlockWhitelist'].split(',')
    except:
      bwl=dict_from_workload['request']['schema']['Task1']['BlockWhitelist']
  else:
    bwl=[]

  if 'RunWhitelist' in dict_from_workload['request']['schema']:
    try:
      rwl=dict_from_workload['request']['schema']['RunWhitelist'].split(',')
    except:
      rwl=dict_from_workload['request']['schema']['RunWhitelist']
  elif 'Task1' in dict_from_workload['request']['schema'] and 'RunWhitelist' in dict_from_workload['request']['schema']['Task1']:
    try:
      rwl=dict_from_workload['request']['schema']['Task1']['RunWhitelist'].split(',')
    except:
      rwl=dict_from_workload['request']['schema']['Task1']['RunWhitelist']
  else:
    rwl=[]
    
  return get_expected_events_withinput(rne,
                                       ids,
                                       bwl,
                                       rwl,
                                       f)

  
def get_expected_events_withinput(
  rne,
  ids,
  bwl,
  rwl,
  filter_eff):
    if rne=='None' or rne==None or rne==0:

        s=0.
        for d in ids:
          if len(rwl):
            try:
                print "$sss %s"%(d)
                ret = generic_get(dbs3_url+"blocks?dataset=%s" %(d)) #returns blocks names
                blocks= ret
            except:
                print d,"does not exist, and therefore we cannot get expected events from it"
                blocks = None

            if blocks:
              for run in rwl:
                ret = generic_get(dbs3_url+"filesummaries?dataset=%s&run_num=%s" %(d, run)) #returns blocks names
                data = ret
                try:
                  s += int(data[0]["num_event"])
                except:
                  print d,"does not have event for",run
          else:
            ret = generic_get(dbs3_url+"blocks?dataset=%s" %(d)) #returns blocks names ????
            blocks = ret
            if len(bwl):
                ##we have a block white list in input
                for b in bwl:
                    if not '#' in b: continue
                    for bdbs in filter(lambda bl: bl["block_name"]==b, blocks):
                        block_data = generic_get(dbs3_url+"blocksummaries?block_name=%s" %(bdbs["block_name"].replace("#","%23"))) #encode # to HTML URL
                        s += block_data[0]["num_event"] #because [1] is about replica of block???
            else:
                for bdbs in blocks:
                    block_data = generic_get(dbs3_url+"blocksummaries?block_name=%s" %(bdbs["block_name"].replace("#","%23"))) #encode # to HTML URL
                    s += block_data[0]["num_event"]
        return s*filter_eff
      
        #work from input dbs and block white list
        if len(bwl):
            s=0.
            for b in bwl:
                if not '#' in b: #a block whitelist needs that
                  continue
                try:
                    ret = generic_get(dbs3_url+"blocksummaries?block_name=%s" %(b.replace("#","%23"))) #encode # to HTML URL
                    data = ret
                    s += data[0]["num_event"]
                except:
                    print b,'does not have events'
            return s*filter_eff
        else:
            s=0.
            for d in ids:
                try:

                    ret = generic_get(dbs3_url+"blocksummaries?dataset=%s" %(d))
                    data = ret
                    s += data[0]["num_event"]
                except:
                    print d,"does not have events"
            return s*filter_eff
    else:
        return rne

#------------------------------------------------------------------------------- 



def timelist_to_str(timelist):
  (h,m,s)=map(int,timelist)[3:]
  return "%02d%02d%02d"%(h,m,s)
  
def datelist_to_str(datelist):
  year=str(datelist[0])[2:]
  month=str(datelist[1])
  if len(month)==1:
    month="0%s"%month
  day=str(datelist[2])
  if len(day)==1:
    day="0%s"%day
  datestr="%s%s%s"%(year,month,day)
  return datestr
  
#------------------------------------------------------------------------------- 

def get_running_days(raw_date): 
  try:
    now=datetime.datetime.now()  
    yy=int(raw_date[0:2])
    mm=int(raw_date[2:4])
    dd=int(raw_date[4:6])
    then=datetime.datetime(int("20%s"%yy),mm,dd,0,0,0)  
    return int((now-then).days)
  except:
    return -1
  
#------------------------------------------------------------------------------- 

def get_campaign_from_prepid(prepid):
  try:
    return prepid.split("-")[1]
  except:
    return "ByHand"
#-------------------------------------------------------------------------------   

def get_status_nevts_from_dbs(dataset):

  undefined=(None,0,0)
  debug=False
  if dataset=='None Yet':
    return undefined
  if dataset=='?':
    return undefined
  
  if debug : print "Querying DBS"

  total_evts=0
  total_open=0

  try:
    ret = generic_get(dbs3_url+"blocks?dataset=%s&detail=true" %(dataset))
    blocks = ret
  except:
    print "Failed to get blocks for --",dataset,"--"
    print traceback.format_exc()
    blocks = []
    return undefined

  for b in blocks:
    if debug:    print b
    if b["open_for_writing"] == 0: #lame DAS format: block info in a single object in list ????
        ret = generic_get(dbs3_url+"blocksummaries?block_name=%s" %(b["block_name"].replace("#","%23")))
        data = ret
        total_evts+=data[0]["num_event"]
    elif b["open_for_writing"] == 1:
        ret = generic_get(dbs3_url+"blocksummaries?block_name=%s" %(b["block_name"].replace("#","%23")))
        data = ret
        total_open+=data[0]["num_event"]
  if debug:    print "done"
  
  try:
    ret = generic_post(dbs3_url+"datasetlist", {"dataset":[dataset], "detail":True})
    result = json.loads(ret) #post method return data as string so we must load it
    if len(result) != 0 :
        status = str(result[0]["dataset_access_type"])
    else:
        status=None
  except:
    print "\n ERROR getting dataset status. return:%s"%(ret)
    raise Exception("  datasetlist POST data:\n%s \n\n results:\n%s"%({"dataset":[dataset], "detail":True}, ret))

  if not status:
    status=None

  return (status,total_evts,total_open)
  
#-------------------------------------------------------------------------------  

#------------------------------------------------------------------------------- 
def get_running_jobs(req):
  #stats_dict={}
  #for key in wma2reason:
    #reason_key=wma2reason[key]
    #quantity=0
    #if req.has_key(key):
      #quantity=req[key]
    #stats_dict[reason_key]=int(quantity)
  key="Running"
  nrunning = 0
  if req.has_key(key):
    nrunning = req[key]
  
  return nrunning

#------------------------------------------------------------------------------- 

def get_pending_jobs(req):  
  key = "Pending"
  bsubmitted=0
  if req.has_key(key):
    bsubmitted = req[key]
  return bsubmitted

#------------------------------------------------------------------------------- 

def get_all_jobs(req):
  key="running"
  all_jobs=0
  if req.has_key(key):
    all_jobs = req[key]
  return all_jobs

#------------------------------------------------------------------------------- 
def calc_eta(level,running_days):
  flevel=float(level)
  irunning_days=int(running_days)
  if flevel>=99.99:
    return 0
  elif flevel<=0.:
    return -1
  #print level
  #print running_days
  total_days=100.*irunning_days/flevel
  eta = total_days-running_days
  return round(eta+0.5)

#------------------------------------------------------------------------------- 

def getDictFromWorkload(req_name, attributes=['request','constraints']):

  dict_from_workload={}
  dict_from_workload=TransformRequestIntoDict( req_name, attributes, True )
  dontloopforever=0
  while dict_from_workload==None:
    print "The request",req_name,"does not have a workLoad ... which is a glitch"
    dict_from_workload=TransformRequestIntoDict( req_name, attributes, True )
    dontloopforever+=1
    if dontloopforever>10:
      print "\t\tI am lost trying to get the dict workLoad for\n\t\t",req_name
      break
  return dict_from_workload










def compare_dictionaries(dict1, dict2):
     if dict1 == None or dict2 == None:
         return False

     if type(dict1) is not dict or type(dict2) is not dict:
         return False

     shared_keys = set(dict2.keys()) & set(dict2.keys())

     if not ( len(shared_keys) == len(dict1.keys()) and len(shared_keys) == len(dict2.keys())):
         return False


     try:
         dicts_are_equal = True
         for key in dict1.keys():
             if type(dict1[key]) is dict:
                 dicts_are_equal = dicts_are_equal and compare_dictionaries(dict1[key],dict2[key])
             else:
                 dicts_are_equal = dicts_are_equal and (dict1[key] == dict2[key])
         return dicts_are_equal
     except:
         return False




class fetcher:
  pdmv_doc_schema = {
    "pdmv_expected_events": 0,
    "pdmv_configs": [ ],
    "pdmv_priority": 0,
    "pdmv_running_jobs": 0,
    "pdmv_running_days": 0,
    "pdmv_evts_in_DAS": 0,
    "pdmv_completion_eta_in_DAS": 0,
    "pdmv_all_jobs": 0,
    "pdmv_status": "",
    "pdmv_status_from_reqmngr": "",
    "pdmv_prep_id": "",
    "pdmv_dataset_name": "",
    "pdmv_custodial_sites": [ ],
    "pdmv_dataset_list": [ ],
    "pdmv_completion_in_DAS": 0,
    "pdmv_request_name": "",
    "pdmv_type": "",
    "pdmv_at_T2": [ ],
    "pdmv_at_T3": [ ],
    "pdmv_dataset_statuses": { },
    "pdmv_running_sites": [ ],
    "pdmv_assigned_sites": [ ],
    "pdmv_submission_date": "",
    "pdmv_campaign": "",
    "pdmv_request": { },
    "pdmv_present_priority": 0,
    "pdmv_pending_jobs": 0,
    "pdmv_input_dataset": "",
    "pdmv_monitor_history": [],
    "pdmv_performance": {},
    "pdmv_status_in_DAS": "",
    "pdmv_monitor_time": "",
    "pdmv_submission_time": "",
    "pdmv_open_evts_in_DAS": 0
    }
  ## from the reqgmr scheme to pdmv stats schema
  just_pass_on = [
    ('RequestName','_id'),
    ('request_name','_id'),
    ('RequestName', 'pdmv_request_name'),
    ('RequestStatus', 'pdmv_status_from_reqmngr'),
    ('status','pdmv_status_from_reqmngr'),
    ('RequestPriority','pdmv_priority'),
    ('Campaign' , 'pdmv_campaign')]
  
  class worker(Thread):
    ""
    def __init__(self, workflow, mother_board):
      Thread.__init__(self)
      self.wf = workflow
      print "prepare",self.wf
      self.mb = mother_board
      self.dict_from_workload = None
      self.dict_from_phedex = None
      self.dict_from_request = None

    def getDictFromPhedex(self):
      if self.dict_from_phedex: return
      ## check with the dataset status
      self.dict_from_phedex=phedex(self.pdmv_request_dict["pdmv_dataset_name"])

    def getDictFromRequest(self):
      if self.dict_from_request: return
      self.dict_from_request={}
      url="/reqmgr/reqMgr/request?requestName="+self.wf
      urlconn='cmsweb.cern.ch'
      conn = httplib.HTTPSConnection(urlconn,
                                     cert_file = os.getenv('X509_USER_PROXY'),
                                     key_file = os.getenv('X509_USER_PROXY')
                                     )
      
      conn.request('GET',url)
      response = conn.getresponse()
      self.dict_from_request = json.loads( response.read()) 
      
    def getDictFromWorkload(self,attributes=['request','constraints']):
      if self.dict_from_workload: return
      self.dict_from_workload={}
      self.dict_from_workload=TransformRequestIntoDict( self.wf, attributes, True )
      dontloopforever=0
      while self.dict_from_workload==None:
        print "The request",self.wf,"does not have a workLoad ... which is a glitch"
        self.dict_from_workload=TransformRequestIntoDict( self.wf, attributes, True )
        dontloopforever+=1
        if dontloopforever>10:
          raise Exception("\t\tI am lost trying to get the dict workLoad for\n\t\t"+self.wf)
      
    def worthTheUpdate(self):
      if self.mb.options.force: return True
      just_different = ['pdmv_status_from_reqmngr',
                        'pdmv_status_in_DAS',
                        #'pdmv_performance',
                        'pdmv_at_T2',
                        'pdmv_at_T3',
                        'pdmv_running_sites',
                        'pdmv_assigned_sites',
                        ]
      for what in  just_different:
        if what not in self.initial_pdmv_request_dict or what not in self.pdmv_request_dict: return True

        if type( self.initial_pdmv_request_dict ) ==list:
          if set(self.initial_pdmv_request_dict[what])!=set(self.pdmv_request_dict[what]): return True
        else:
          if self.initial_pdmv_request_dict[what]!=self.pdmv_request_dict[what]: return True
      
      def for_a_ds( old, new ):
        previous = float(old['pdmv_evts_in_DAS']+old['pdmv_open_evts_in_DAS'])
        now = float(new['pdmv_evts_in_DAS']+new['pdmv_open_evts_in_DAS'])
        if now:
          f_more = (now-previous)/now
          if f_more > 0.04:
            return True

      for ds in self.pdmv_request_dict['pdmv_dataset_statuses']:
        if ds in self.initial_pdmv_request_dict['pdmv_dataset_statuses']:
          if for_a_ds(self.initial_pdmv_request_dict['pdmv_dataset_statuses'][ds], self.pdmv_request_dict['pdmv_dataset_statuses'][ds]):
            return True

    def update(self):
      """
      This is the big thing
      """
      print "Updating",self.wf      
      
      try:
        self.initial_pdmv_request_dict = self.mb.statsCouch.get_file_info(self.wf)
      except:
        print "document",self.wf,"does not exist yet in stats"
        self.getDictFromRequest()
        self.initial_pdmv_request_dict = self.mb.new_doc( self.dict_from_request )
        #self.initial_pdmv_request_dict = self.mb.statsCouch.get_file_info(self.wf)

      self.pdmv_request_dict = copy.deepcopy( self.initial_pdmv_request_dict )

      ## add current time
      self.pdmv_request_dict["pdmv_monitor_time"]=time.asctime()
      
      ## what are the outputs
      if not self.pdmv_request_dict["pdmv_dataset_list"]:
        self.pdmv_request_dict["pdmv_dataset_list"] = get_dataset_list( self.wf )
      if self.pdmv_request_dict["pdmv_dataset_list"]:
        self.pdmv_request_dict["pdmv_dataset_name"] = self.pdmv_request_dict["pdmv_dataset_list"][0]

      ## get priorities : initial and current
      self.getDictFromWorkload()
      self.getDictFromRequest()
      self.getDictFromPhedex()
      self.pdmv_request_dict['pdmv_request'] = self.dict_from_request
      self.pdmv_request_dict['pdmv_priority']=self.dict_from_request['RequestPriority']
      self.pdmv_request_dict['pdmv_present_priority']=self.dict_from_workload['request']['priority']

      ## basic things
      self.pdmv_request_dict["pdmv_prep_id"] = self.dict_from_request['PrepID']
      self.pdmv_request_dict["pdmv_campaign"] = self.dict_from_request['Campaign']
      self.pdmv_request_dict['pdmv_type'] = self.dict_from_request['RequestType']
      self.pdmv_request_dict["pdmv_status_from_reqmngr"] = self.dict_from_request['RequestStatus']

      ## getting sites :
      sites=set()
      for tn in [k for k in self.dict_from_workload['tasks'] if 'constraints' in self.dict_from_workload['tasks'][k]]:
        sites.update(self.dict_from_workload['tasks'][tn]['constraints']['sites']['whitelist'])
      self.pdmv_request_dict['pdmv_custodial_sites']= custodials(self.dict_from_phedex)
      if not self.pdmv_request_dict['pdmv_custodial_sites']:
        self.pdmv_request_dict['pdmv_custodial_sites']=filter(lambda s : s.startswith('T1_'), sites)
      self.pdmv_request_dict['pdmv_running_sites']=runningSites(self.dict_from_phedex)
      self.pdmv_request_dict['pdmv_assigned_sites']=list(sites)
      self.pdmv_request_dict['pdmv_at_T2']=atT2(self.dict_from_phedex)
      self.pdmv_request_dict['pdmv_at_T3']=atT3(self.dict_from_phedex)

      ## initial times
      self.pdmv_request_dict['pdmv_submission_date'] = datelist_to_str( self.dict_from_request['RequestDate'] )
      self.pdmv_request_dict['pdmv_submission_time'] = timelist_to_str( self.dict_from_request['RequestDate'] ) 

      ## times running
      self.pdmv_request_dict['pdmv_running_days'] = get_running_days( self.pdmv_request_dict['pdmv_submission_date'] )

      ## expected statistics
      self.pdmv_request_dict['pdmv_expected_events_per_ds'] = get_expected_events_by_output( self.wf )
      
      ## get current statistics
      self.pdmv_request_dict['pdmv_dataset_statuses'] = {}
      for other_ds in self.pdmv_request_dict['pdmv_dataset_list']:
        other_status,other_evts,other_openN = get_status_nevts_from_dbs( other_ds )
        self.pdmv_request_dict['pdmv_dataset_statuses'][other_ds] = {
          'pdmv_status_in_DAS' : other_status,
          'pdmv_evts_in_DAS' : other_evts,
          'pdmv_open_evts_in_DAS' : other_openN,
          'pdmv_expected_events' : 0,
          'pdmv_completion_in_DAS' : 0,
          'pdmv_completion_eta_in_DAS' : 0
          }
        if other_ds in self.pdmv_request_dict['pdmv_expected_events_per_ds']:
          self.pdmv_request_dict['pdmv_dataset_statuses'][other_ds]['pdmv_expected_events'] = self.pdmv_request_dict['pdmv_expected_events_per_ds'][other_ds]
          if self.pdmv_request_dict['pdmv_dataset_statuses'][other_ds]['pdmv_expected_events']:
            self.pdmv_request_dict['pdmv_dataset_statuses'][other_ds]['pdmv_completion_in_DAS'] = float("%2.2f" % (
                100* float(self.pdmv_request_dict['pdmv_dataset_statuses'][other_ds]['pdmv_evts_in_DAS'])/self.pdmv_request_dict['pdmv_dataset_statuses'][other_ds]['pdmv_expected_events']))
            ## could make a much better ETA calculation here
            self.pdmv_request_dict['pdmv_dataset_statuses'][other_ds]['pdmv_completion_eta_in_DAS'] = calc_eta( self.pdmv_request_dict['pdmv_dataset_statuses'][other_ds]['pdmv_completion_in_DAS'], self.pdmv_request_dict['pdmv_running_days'])
          

      ## presented value. to be deprecated, very likely
      if self.pdmv_request_dict['pdmv_dataset_name'] in self.pdmv_request_dict['pdmv_dataset_statuses']:
        for (k,v) in self.pdmv_request_dict['pdmv_dataset_statuses'][ self.pdmv_request_dict['pdmv_dataset_name'] ].items():
          self.pdmv_request_dict[k]=v
      if not self.pdmv_request_dict['pdmv_expected_events']:
        self.pdmv_request_dict['pdmv_expected_events'] = get_expected_events_withdict( self.dict_from_workload )

      ## performance report
      self.pdmv_request_dict['pdmv_performance']=Performances( self.wf )
      ## number of jobs : no-one use it ? broken source
      ## configs : only works for taskchains
      self.pdmv_request_dict['pdmv_configs'] = configsFromWorkload( self.wf )

      ## create the history
      to_get=['pdmv_monitor_time','pdmv_evts_in_DAS','pdmv_open_evts_in_DAS','pdmv_status_in_DAS','pdmv_dataset_statuses']
      rev={}
      for g in to_get:
        if not g in self.pdmv_request_dict: continue
        rev[g] = copy.deepcopy(self.pdmv_request_dict[g])
      
      if len(self.pdmv_request_dict['pdmv_monitor_history']):
        old_history = copy.deepcopy( self.pdmv_request_dict['pdmv_monitor_history'][0])
        new_history = copy.deepcopy(rev)
        old_history.pop('pdmv_monitor_time')
        new_history.pop('pdmv_monitor_time')
        if not compare_dictionaries(old_history, new_history):
          self.pdmv_request_dict['pdmv_monitor_history'].insert(0, rev)
      else:
        self.pdmv_request_dict['pdmv_monitor_history'].insert(0, rev)
                  
      #evolution plots. does not matter to put more points
      if self.mb.options.plot:
        plotGrowth( self.pdmv_request_dict, None )

      # put it back to db
      if self.worthTheUpdate():
        print "\t\t\tPutting in the db",self.wf
        self.mb.statsCouch.update_file( self.wf, json.dumps( self.pdmv_request_dict ))

      ## poll back mcm
      if self.mb.options.inspect:
        self.mb.mcm.get('restapi/requests/inspect/%s'%(pdmv_request_dict['pdmv_prep_id']))
      
      print "\t\t",self.pdmv_request_dict['pdmv_request_name'],"updated"

    def run(self):
      try:
        self.update()
      except:
        print self.wf,"IS A DEAD FAILING REQUEST"
        trf='traceback/traceback_%s.txt'%(self.wf)
        print trf
        tr=open(trf,'w')
        tr.write(traceback.format_exc())
        tr.close()

        

  def get_reqmgr_docs(self,veto=None, reload=False):
    if self.reqmgr_docs and not reload: 
      return
    ## get the content of wmstats here
    self.reqmgr_docs = get_requests_list()
    self.reqmgr_docs = filter( lambda req : "status" in req, self.reqmgr_docs) ## malformed ones    
    if veto:
      self.reqmgr_docs = filter( lambda req : req["request_name"] not in veto, self.reqmgr_docs)

    print "Got",len(self.reqmgr_docs),"docs from request manager"

  def get_stats_docids(self, reload=False):
    ## get the docids in stats
    if self.stats_docids and not reload:
      return
    allDocs_ = self.statsCouch.get_view('all')
    self.stats_docids = filter(lambda doc : not doc.startswith('_'), [doc['id'] for doc in allDocs_['rows']])

  def __init__( self):
    self.parse_options()
    self.reqmgr_docs=None
    self.statsCouch=Interface(self.options.db)
    self.stats_docids = None
    self.mcm = restful(dev=False)
    self.max_worker = 10
    self.worker_pool = []
    self.work_list = set()


  def n_active( self ):
    n=0
    for w in self.worker_pool: if w.is_alive(): n+=1
    return n

  def new_doc(self, with_info):
    new_doc = copy.deepcopy( self.pdmv_doc_schema )
    for (source,target) in self.just_pass_on:
      if source in with_info:
        new_doc[target] = copy.deepcopy(with_info[source])
    return new_doc
    
  def insert(self):
    """
    Figure out what are the missing docs in stats and adds a bare schema doc
    """
    self.get_stats_docids()
    self.get_reqmgr_docs()

    ##make it linear as there is not much operation here
    for each in self.reqmgr_docs:
      if each['request_name'] in self.stats_docids: continue
      new_doc = self.new_doc( each )
    print "putting doc",new_doc['_id']
    self.statsCouch.create_file( json.dumps( new_doc ) )
    ## for the update later
    self.work_list.add( each['request_name'] )
    
    ## and update all added docs
    self.update()

  def wait_for_next_slot(self,to=1):
    while self.n_active()>= self.max_worker: time.sleep(to)

  def wait_for_completion(self,to=1):
    while self.n_active()!=0:      time.sleep(to)

  def update(self):
    """
    Full update cycle
    """     
    print len(self.work_list),"on the work list, for ",self.max_worker,"workers"
    for (n,each) in enumerate(self.work_list):
      self.wait_for_next_slot()
      print "%d/%d] %s"%(n,len(self.work_list), each)
      self.worker_pool.append( self.worker( each , self ))
      self.worker_pool[-1].start()
      time.sleep(1)

    self.wait_for_completion()

    
  def see_all_in_stats_status(self, status):
    ## get the list of docids in the given status : no wild card
    self.work_list.update(filter(lambda doc : not doc.startswith('_'), [doc['id'] for doc in self.statsCouch.get_view('status', '?key="%s"' %status)['rows']]))

  def see_all_running(self):
    self.see_all_in_stats_status('running-closed')
    self.see_all_in_stats_status('running-open')
      
  def see_all_skewed(self):
    ## get the doc and decide ?
    print "Not implemented yet. No work done"

  def see_all_submitted(self):
    ##caveat, only the one registered in McM will get updated.
    rs = self.mcm.getA('requests', query='status=submitted')
    for r in rs:
      self.work_list.update([wma['name'] for wma in r['reqmgr_name']])

  def see_wild_card(self):
    ## from the search option
    self.get_stats_docids()
    self.work_list = filter( lambda d : self.options.search in d, self.stats_docids)

  def parse_options(self):
    usage= "Usage:\n %prog options"
    parser = optparse.OptionParser(usage)
    parser.add_option("--search",
                      default=None
                      )
    parser.add_option("--force",
                      default=False,
                      action='store_true'
                      )
    parser.add_option("--plot",
                      default=False,
                      action='store_true'
                      )
    parser.add_option("--inspect",
                      default=False,
                      action='store_true'
                      )
    parser.add_option("--do",
                      choices=['update','insert']
                      )
    parser.add_option("--db",
                      default="http://cms-pdmv-stats.cern.ch:5984/stats"
                      )
    parser.add_option("--mcm",
                      default=False,
                      help="drives the update from submitted requests in McM",
                      action="store_true")
    self.options,args=parser.parse_args()
    
    if self.options.mcm and self.options.search:
      print "--search and --mcm are not compatible"
      sys.exit(1)
      
    
  def go(self):
    if self.options.do == 'insert':
      self.insert()
    elif self.options.do == 'update':
      if self.options.mcm:
        self.see_all_submitted()
      else:
        if self.options.search:
          ## we have a wild card 
          self.see_wild_card()
        else:
          self.see_all_running()
      self.update()

if __name__ == "__main__": 
  fetch = fetcher()
  fetch.go()

  
  
