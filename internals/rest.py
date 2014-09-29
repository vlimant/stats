import cherrypy
import json
import urllib2
import traceback
import sys
import os
import traceback

sys.path.append('/afs/cern.ch/cms/slc5_amd64_gcc462/cms/dbs-client/DBS_2_1_1_patch1_1/lib')
from tools.driveUpdate import main_do

class RestIndex(object):
    def __init__(self):
        pass
    def index(self):
        return "alive"

    @cherrypy.expose
    def default(self, *vpath, **params):
        method = getattr(self, cherrypy.request.method, None)
        if not method:
            raise cherrypy.HTTPError(405, "Method not implemented.")
        return method(*vpath, **params); 

class GetOne(RestIndex):
    def __init__(self):
        pass
    def GET(self, *args):
        if not args:
            return dumps("No argument given")
        f = urllib2.urlopen('http://cms-pdmv-stats:5984/stats/%s'%( args[0] ))
        data = f.read()
        return data

class ProducesDN(RestIndex):
    def __init__(self):
        pass
    def GET(self, *args):
        #data = [args[0],args[1]]
        __dname = "/".join(args)
        f = urllib2.urlopen('http://cms-pdmv-stats:5984/stats/_fti/_design/lucene/search?q=DN:/%s&include_docs=true' %(__dname))
        data = f.read()
        return data

class searchOption(object):
    def __init__(self, search):
        self.db = 'http://cms-pdmv-stats.cern.ch'
        self.do = 'update'
        self.force = True
        self.search = search
        self.test = False


class UpdateOne(RestIndex):
    def __init__(self):
        pass
    def GET(self, *args):
        if not args:
            return dumps("No argument given")
        #f = urllib2.urlopen('http://cms-pdmv-stats:5984/stats/%s'%( args[0] ))
        #data = f.read()
        
        #from DBSAPI.dbsApi import DbsApi
        #dbsapi = DbsApi()
        #content = json.loads( data )
        #blocks = dbsapi.listBlocks( str( content['pdmv_dataset_name']))

        print "Getting main_do"
        opt = searchOption( args[0] )
        print "Running main_do"
        main_do ( opt )
        print "Done and moving along"
        
        ## get the data, now that it updated 
        f = urllib2.urlopen('http://cms-pdmv-stats:5984/stats/%s'%( args[0] ))
        data = f.read()
        
        return data 
    
    
