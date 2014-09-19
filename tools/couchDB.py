import urllib2
import json

class Interface(object):
    
    def __init__(self, DB_url):
       self.url_address = DB_url
       self.opener = urllib2.build_opener(urllib2.HTTPHandler)
    
    def update_file(self, fileID, stringToPut):
        ##string must be a updated file name with the newest revision, it will be updated by DB itself
        request = urllib2.Request(self.url_address+'/'+fileID, data=stringToPut)
        request.add_header('Content-Type', 'text/plain')
        request.get_method = lambda: 'PUT'
        url = self.opener.open(request)
        return json.loads(url.read())
        
    def get_all_files(self):
        request = urllib2.Request(self.url_address+'/_all_docs')
        request.add_header('Content-Type', 'text/plain')
        request.get_method = lambda: 'GET'
        try:
            url = self.opener.open(request)
        except:
            print "the url",self.url_address,"is not responding"
            return {'rows':[]}
        return json.loads(url.read())

    def delete_file_info(self, fileID,rev):
        request = urllib2.Request(self.url_address+'/'+fileID+'?rev='+rev)
        request.add_header('Content-Type', 'text/plain')
        request.get_method = lambda: 'DELETE'
        try:
            url = self.opener.open(request)
            return json.loads(url.read())
        except:
            print "Failed delete"
    
    def get_file_info(self, fileID,trail=''):
        request = urllib2.Request(self.url_address+'/'+fileID+trail)
        request.add_header('Content-Type', 'text/plain')
        request.get_method = lambda: 'GET'
        url = self.opener.open(request)
        return json.loads(url.read())

    def get_file_info_rev(self,fileID,rev):
        return self.get_file_info(fileID, trail='?rev='+rev)
        
    def get_file_info_withrev(self, fileID):
        return self.get_file_info(fileID, trail='?revs_info=true')
        
    def create_file(self, stringToPut):
        request = urllib2.Request(self.url_address, data=stringToPut)
        request.add_header('Content-Type', 'application/json')
        request.get_method = lambda: 'POST'
        url = self.opener.open(request)
        return json.loads(url.read())

    def get_view(self, view, params=None):

        if params:
            url = self.url_address+'/_design/stats/_view/'+view+params
        else:
            url = self.url_address+'/_design/stats/_view/'+view
        print url
        request = urllib2.Request(url)
        request.add_header('Content-Type', 'text/plain')
        request.get_method = lambda: 'GET'
        url = self.opener.open(request)
        return json.loads(url.read())
