#!/usr/bin/env python

from couchDB import Interface
import json
import pprint
import multiprocessing
import itertools
import copy
import optparse
import time
import traceback
import os
import sys

FORCE=False

def insertAll(req_list,docs,pattern=None,limit=None):
    newentries=0
    for req in req_list:
        docid=req["request_name"]
        ## not already present
        if docid in docs:            continue
        ## not broken
        if not "status" in req:            continue
        ## not aborted or rejected
        if req["status"] in ['aborted','rejected']:        continue
        ## not according to pattern search
        if pattern and not pattern in docid:            continue
        ## to limit things a bit
        if limit and newentries>=limit:    break
        
        #print docid,"is not in the stats cache yet"
        if insertOne(req):
            newentries+=1
    print newentries,"inserted"

countOld=0
def insertOne(req):
    global statsCouch
    from statsMonitoring import parallel_test
    updatedDoc=parallel_test( [req,[]] )
    docid=req["request_name"]
    if updatedDoc==None or not len(updatedDoc):
        print "failed to get anything for",docid
        return False
    updatedDoc['_id'] = docid
    statsCouch.create_file(json.dumps(updatedDoc))
    #pprint.pprint(updatedDoc)
    return docid

def worthTheUpdate(new,old):
    ##make a tighter selection on when to update in couchDB to not overblow it with 2 update per hour ...
    global FORCE
    global countOld
    if FORCE: 
        return True

    if old['pdmv_evts_in_DAS']!=new['pdmv_evts_in_DAS']:
        return True
    if old['pdmv_status_in_DAS']!=new['pdmv_status_in_DAS']:
        return True
    if old['pdmv_status_from_reqmngr']!=new['pdmv_status_from_reqmngr']:
        return True

    if set(old['pdmv_at_T2'])!=set(new['pdmv_at_T2']):
        return True

    if set(old['pdmv_at_T3'])!=set(new['pdmv_at_T3']):
        return True

    
    if old!=new:
        ## what about monitor time ???? that is different ?
        if set(old.keys())!=set(new.keys()):
            ## addign a new parameter
            return True
        if 'pdmv_performance' in new and new['pdmv_performance']!={}:
            if not 'pdmv_performance' in old:
                return True
            else:
                if new['pdmv_performance']!= old['pdmv_performance']:
                    return True

        #samples location has updated
        if ('pdmv_at_T2' in old and 'pdmv_at_T2' in new) and (set(old['pdmv_at_T2']) != set(new['pdmv_at_T2']) ):
            return True
        if ('pdmv_at_T3' in old and 'pdmv_at_T3' in new) and (set(old['pdmv_at_T3']) != set(new['pdmv_at_T3']) ):
            return True
                
        n_more=(new['pdmv_evts_in_DAS']+new['pdmv_open_evts_in_DAS'])-(old['pdmv_evts_in_DAS']+old['pdmv_open_evts_in_DAS'])
        n_tot=float(new['pdmv_evts_in_DAS']+new['pdmv_open_evts_in_DAS'])
        f_more=-1
        if n_tot:
            f_more=n_more / n_tot
        #more than x% more stat
        if f_more > 0.04:
            return True
        #change of status
        if old['pdmv_status_from_reqmngr']!=new['pdmv_status_from_reqmngr']:
            return True
        if old['pdmv_running_days']!=new['pdmv_running_days']:
            return True

        uptime=time.mktime(time.strptime(new['pdmv_monitor_time']))
        oldtime=time.mktime(time.strptime(old['pdmv_monitor_time']))
        deltaUpdate = (uptime-oldtime) / (60. * 60. * 24.)
        ### more than 10 days old => update
        if deltaUpdate>10 and countOld<=30:
            countOld+=1
            return True
        ##otherwise do not update, even with minor changes
        print "minor changes to",new['pdmv_request_name'],n_more,"more events for",f_more
        print old['pdmv_status_from_reqmngr'], new['pdmv_status_from_reqmngr']
        return False
    else:
        return False

def compare_dictionaries(dict1, dict2):
     if dict1 == None or dict2 == None:
         return False

     if type(dict1) is not dict or type(dict2) is not dict:
         return False

     shared_keys = set(dict2.keys()) & set(dict2.keys())

     if not ( len(shared_keys) == len(dict1.keys()) and len(shared_keys) == len(dict2.keys())):
         return False


     dicts_are_equal = True
     for key in dict1.keys():
         if type(dict1[key]) is dict:
             dicts_are_equal = dicts_are_equal and compare_dictionaries(dict1[key],dict2[key])
         else:
             dicts_are_equal = dicts_are_equal and (dict1[key] == dict2[key])

     return dicts_are_equal

def updateOne(docid,req_list):
    global statsCouch
    match_req_list=filter (lambda r: r["request_name"]==docid, req_list)
    try:
        thisDoc=statsCouch.get_file_info(docid)
    except:
        print "There was an access crash with",docid
        return False
    
    updatedDoc=copy.deepcopy(thisDoc)
    #print "before update"
    #pprint.pprint(thisDoc)
    if not len(match_req_list):
        ## when there is a fake requests in stats.
        if docid.startswith('fake_'):
            match_req_list=[{"request_name":docid, "status":"announced","type":"ReDigi"}]
            print "FAKE"
        return False
    if len(match_req_list)>1 :
        print "more than one !!"
    req=match_req_list[0]
    from statsMonitoring import parallel_test
    global FORCE
    updatedDoc=parallel_test( [req,[updatedDoc]] ,force=FORCE)
    #print "updated"
    #pprint.pprint(updatedDoc)
    if updatedDoc=={}:
        print "updating",docid,"returned an empty dict"
        return False
    if updatedDoc==None:
        print "deleting",docid
        pprint.pprint(thisDoc)
        statsCouch.delete_file_info(docid,thisDoc['_rev'])
        return False

    ## minimalistic output
    #if not 'pdmv_evts_in_DAS' in updatedDoc:
    #    print "This is a minimalistic doc"
    #    return False

    #if pprint.pformat(updatedDoc)!=pprint.pformat(thisDoc):
    if worthTheUpdate(updatedDoc,thisDoc):
        to_get=['pdmv_monitor_time','pdmv_evts_in_DAS','pdmv_open_evts_in_DAS','pdmv_dataset_statuses']
        if 'pdvm_monitor_history' in updatedDoc:
            updatedDoc['pdmv_monitor_history'] = updatedDoc.pop( 'pdvm_monitor_history' )
        if not 'pdmv_monitor_history' in updatedDoc:
            ## do the migration
            thisDoc_bis = statsCouch.get_file_info_withrev(docid)
            revs = thisDoc_bis['_revs_info']
            history=[]
            for rev in revs:
                try:
                    nextOne=statsCouch.get_file_info_rev(docid, rev['rev'])
                except:
                    continue

                history.append({})
                for g in to_get:
                    if not g in nextOne: continue
                    history[-1][g] = copy.deepcopy(nextOne[g])

            updatedDoc['pdmv_monitor_history'] = history
        if 'pdmv_monitor_history' in updatedDoc:
            rev = {}
            for g in to_get:
                if not g in updatedDoc: continue
                rev[g] = copy.deepcopy(updatedDoc[g])
            old_history = copy.deepcopy(updatedDoc['pdmv_monitor_history'][0])
            new_history = copy.deepcopy(rev)
            del old_history["pdmv_monitor_time"] ##compare history without monitor time
            del new_history["pdmv_monitor_time"]
            if not compare_dictionaries(old_history, new_history): # it is worth to fill history
                updatedDoc['pdmv_monitor_history'].insert(0, rev)

        
        try:
            statsCouch.update_file(docid,json.dumps(updatedDoc))
            #pprint.pprint(updatedDoc)
            #print updatedDoc['pdmv_at_T2']
            print docid,"something has changed"
            return docid
        except:
            print "Failed to update",docid
            print traceback.format_exc()
            return False
    else:
        print docid,"nothing changed"
        return False

def updateOneIt(arguments):
    #print len(arguments)
    #return 
    docid,req_list = arguments
    return updateOne(docid,req_list)
    
def updateSeveral(docs,req_list,pattern=None):
    for docid in docs:
        if pattern and not pattern in docid:
            continue
        print docid
        updateOne(docid,req_list)

def dumpSome(docids,limit):
    dump=[]
    for docid in docids:
        if limit and len(dump)>= limit: break
        try:
            a_doc = statsCouch.get_file_info( docid)
            dump.append( a_doc )
            dump[-1].pop('_id')
            dump[-1].pop('_rev')
        except:
            pass
        #dump.append( statsCouch.get_file_info( docid) )
        #dump[-1].pop('_id')
        #dump[-1].pop('_rev')
    return dump

docs=[]
statsCouch=None

def main():
    global docs,FORCE,statsCouch
    usage= "Usage:\n %prog options"
    parser = optparse.OptionParser(usage)
    parser.add_option("--search",
                      default=None
                      )
    parser.add_option("--test",
                      default=False,
                      action='store_true'
                      )
    parser.add_option("--force",
                      default=False,
                      action='store_true'
                      )
    parser.add_option("--inspect",
                      default=False,
                      action='store_true'
                      )
    parser.add_option("--do",
                      choices=['update','insert','store','kill','list'],
                      )
    parser.add_option("--db",
                      default="http://cms-pdmv-stats.cern.ch")
    parser.add_option("--mcm",
                      default=False,
                      help="drives the update from submitted requests in McM",
                      action="store_true")
    parser.add_option("--nowmstats",
                      default=False,
                      help="Goes back to query the full content of request manager",
                      action="store_true")
    parser.add_option("--check",
                      default=False,
                      help="Prevent two from running at the same time",
                      action="store_true")
    
    options,args=parser.parse_args()

    return main_do( options )

def main_do( options ):

    if options.check:
        checks=['ps -f -u $USER']
        for arg in sys.argv[1:]:
            checks.append('grep "%s"'%(arg.split('/')[-1].replace('--','')))
        checks.append('grep -v grep')
        c = " | ".join(checks)
        check=filter(None,os.popen("|".join(checks)).read().split('\n'))
        if len(check)!=1:
            print "already running with that exact setting"
            print check
            sys.exit(1)
        else:
            print "ok to operate"
    
    start_time = time.asctime()
    global statsCouch,docs,FORCE
    #interface to the couchDB
    statsCouch=Interface(options.db+':5984/stats')


    ## get from stats couch the list of requests
    print "Getting all stats ..."
    #allDocs=statsCouch.get_all_files()
    allDocs = statsCouch.get_view('all')
    docs = [doc['id'] for doc in allDocs['rows']]
    #remove the _design/stats
    docs = filter(lambda doc : not doc.startswith('_'), docs)
    print "... done"

    nproc=5
    limit=None
    if options.test:
        limit=10
        
    if options.do == 'insert':
        ## get from wm couch
        from statsMonitoring import parallel_test,get_requests_list
        print "Getting all req ..."
        req_list = get_requests_list()
        print "... done"
        
        ## insert new requests, not already in stats couch into stats couch
        #insertAll(req_list,docs,options.search,limit)

        if options.search:
            req_list = filter( lambda req : options.search in req["request_name"], req_list )
            #print len(req_list)

        #skip malformated ones
        req_list = filter( lambda req : "status" in req, req_list )
        #print len(req_list)
        
        #take only the ones not already in there
        req_list = filter( lambda req : req["request_name"] not in docs, req_list )
        #print len(req_list)
            
        #skip trying to insert aborted and rejected or failed
        #req_list = filter( lambda req : not req["status"] in ['aborted','rejected','failed','aborted-archived','rejected-archived','failed-archived'], req_list )
        req_list = filter( lambda req : not req["status"] in ['aborted','rejected','failed'], req_list )
        #print len(req_list)
            
        #do not update TaskChain request statuses
        #req_list = filter( lambda req : 'type' in req and req['type']!='TaskChain', req_list)
        #print len(req_list)

        pprint.pprint( req_list)
            
        if limit:
            req_list = req_list[0:limit]
            #print len(req_list)
            
        newentries=0
        print "Dispaching",len(req_list),"requests to",str(nproc),"processes..."
        pool = multiprocessing.Pool(nproc)
        results=pool.map( insertOne, req_list )
        print "End dispatching!"

        results=filter( lambda item : item!=False, results)
        print len(results),"inserted"
        print str(results)
        """
        showme=''
        for r in results:
            showme+='\t'+r+'\n'
        print showme
        """
    elif options.do =='kill' or options.do =='list' :
        ## get from wm couch
        from statsMonitoring import parallel_test,get_requests_list
        print "Getting all req ..."        
        req_list = get_requests_list()
        print "... done"

        removed=[]
        if options.search:
            req_list=filter( lambda req : options.search in req["request_name"], req_list)
            for r in req_list:
                print "Found",r['request_name'],"in status",(r['status'] if 'status' in r else 'undef'),"?"
                if options.do =='kill':
                    #print "killing",r['request_name'],"in status",(r['status'] if 'status' in r else 'undef'),"?"
                    docid=r['request_name']
                    if docid in docs and not docid in removed:
                        thisDoc=statsCouch.get_file_info(docid)
                        print "removing record for docid"
                        statsCouch.delete_file_info(docid,thisDoc['_rev'])
                        removed.append(docid)
                    else:
                        print "nothing to kill"
                
    elif options.do == 'update':
        ## get from wm couch
        from statsMonitoring import parallel_test,get_requests_list
        print "Getting all req ..."
        req_list = get_requests_list( not_in_wmstats = options.nowmstats)
        print "... done"
        
        ## unthreaded
        #updateSeveral(docs,req_list,pattern=None)

        if options.mcm:
            sys.path.append('/afs/cern.ch/cms/PPD/PdmV/tools/McM/')
            from rest import restful
            mcm = restful(dev=False,cookie='/afs/cern.ch/user/p/pdmvserv/private/prod-cookie.txt')
            rs = mcm.getA('requests', query='status=submitted')
            rids = map(lambda d : d['prepid'], rs)

            print "Got",len(rids),"to update from mcm"
            #print len(docs),len(req_list)
            #print map( lambda docid : any( map(lambda rid : rid in doc, rids)), docs)
            docs=filter( lambda docid : any( map(lambda rid : rid in docid, rids)), docs)
            if not len(docs):
                req_list=filter( lambda req : any( map(lambda rid : rid in req["request_name"], rids)), req_list)

        if options.search:
            if options.force:
                FORCE=True
            docs=filter( lambda docid : options.search in docid, docs)
            if not len(docs):
                req_list=filter( lambda req : options.search in req["request_name"], req_list)
                if len(req_list):
                    pprint.pprint(req_list)
        if limit:
            docs = docs[0:limit]
            
        repeated_req_list = itertools.repeat( req_list , len(docs) )
        print "Dispaching",len(docs),"requests to ",str(nproc),"processes..."
        pool = multiprocessing.Pool(nproc)
        results=pool.map( updateOneIt, itertools.izip( docs, repeated_req_list ) )
        print "End dispatching!"

        if options.search:
            dump=dumpSome(docs,limit)
            print "Result from update with search"
            pprint.pprint(dump)
            
        results=filter( lambda item : item!=False, results)
        print len(results),"updated"
        print results

        print "\n\n"
        ##udpdate the growth plots ???
        from growth import plotGrowth
        for r in results:
            try:
                withRevisions=statsCouch.get_file_info_withrev(r)
                plotGrowth(withRevisions,statsCouch,force=FORCE)
                ## notify McM for update !!
                if (withRevisions['pdmv_prep_id'].strip() not in ['No-Prepid-Found','','None']) and options.inspect and '_' not in withRevisions['pdmv_prep_id']:
                    inspect='curl -s -k --cookie ~/private/prod-cookie.txt https://cms-pdmv.cern.ch/mcm/restapi/requests/inspect/%s' % withRevisions['pdmv_prep_id']
                    os.system(inspect)
            except:
                print "failed to update growth for",r
                print traceback.format_exc()


        print "\n\n"
        ## set in the log file
        #serves as forceupdated !
        print "start time: ", start_time
        print "logging updating time:",time.asctime()
        l=open('stats.log','a')
        l.write(time.asctime()+'\n')
        l.close()
        
    elif options.do == 'store':

        from statsMonitoring import print_stats
        
        if options.search:
            docs=filter( lambda docid : options.search in docid, docs)

        dump=dumpSome(docs,limit)
        f=open('stats_json.txt','w')        
        f.write(json.dumps(dump, indent=4))
        f.close()

        print len(dump),"put in the flat file dump of the couch db"

        stats_page = print_stats(dump)
        stats_file=open("/afs/cern.ch/cms/PPD/PdmV/web/stats/index.html","w")
        stats_file.write(stats_page)
        stats_file.close()

        print "stats static page created"

if __name__ == "__main__":
    PROFILE=False
    if PROFILE:
        import cProfile
        cProfile.run('main()')
    else:
        main()
    
