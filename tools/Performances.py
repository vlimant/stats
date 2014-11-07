import urllib2,httplib,os
import pprint
import json

def Performances(request):
    url="/couchdb/workloadsummary/"+request
    urlconn='cmsweb.cern.ch'
    conn = httplib.HTTPSConnection(urlconn,
                                   cert_file = os.getenv('X509_USER_PROXY'),
                                   key_file = os.getenv('X509_USER_PROXY')
                                   )
    
    conn.request('GET',url)
    response = conn.getresponse()
    try:
        result=json.loads(response.read())
    except:
        print "The site is probably down"
        print response.read()
        return {}
    #pprint.pprint(result)
    #print 'Request: %s'%(request)
    r_result={}
    try:
        tasks=map(str,result['performance'].keys())
    except:
        pprint.pprint(result)
        return r_result
    #pprint.pprint(tasks)
    for task in tasks:
        task=task.split('/',2)[-1]
        next=1
        while next:
            try:
                time_per_event_bins=result['performance']['/%s/%s'%(request,task)]['cmsRun%d'%(next)]['AvgEventTime']['histogram']
            except:
                #print task,"does not have performance"
                next=None
                continue
            
            time_per_event=0
            count=0
            for bin in time_per_event_bins:
                time_per_event+=bin['average'] * bin['nEvents']
                count+=bin['nEvents']
            if count:
                time_per_event/=count
                r_result[task+'_%d'%(next)]=time_per_event
            next+=1

    return r_result

if __name__ == "__main__":

     request='franzoni_EflowHpu_NuGun25ns_RAWRECOSIMHLT_130219_175620_5038'
     request='nnazirid_BTV-Summer12_DR53X-00031_T1_TW_ASGC_MSS_105_v1__130213_184430_4540'
     request='efajardo_ACDC_BTV-Summer12_DR53X-00026_T1_TW_ASGC_MSS_97_v1___130207_112221_5777'
     request='casarsa_EXO-Summer12-01739_147_v1__120913_212229_9605'
     
     d=Performances(request)

     print 'Request: %s'%(request)
     for (task,time_per_event) in d.items():
         print '\t TaskRequest: %s  %.2f s/event'%(task,time_per_event)
