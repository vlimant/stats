import urllib2,httplib,os
import pprint
import json
import traceback

def dict2obj(d):
    if not isinstance(d, dict):
        return d
    class C(object):
        def __str__(self):
            return self.rep
        pass
    o = C()
    for k in d:
        o.__dict__[k] = dict2obj(d[k])
    import pprint
    o.__dict__['rep']=pprint.pformat(d)
    return o

def getlines(request):
    import os
    if os.path.exists(request+".workload"):
        return open(request+".workload").read().split('\n')
        
    url="/reqmgr/view/showWorkload?requestName="+request
    urlconn='cmsweb.cern.ch'
    conn = httplib.HTTPSConnection(urlconn,
                                   cert_file = os.getenv('X509_USER_PROXY'),
                                   key_file = os.getenv('X509_USER_PROXY')
                                   )
    
    conn.request('GET',url)
    response = conn.getresponse()

    lines=[]
    for l in response.read().split('<br/>'):
        if 'section_' in l:            continue
        if 'Cannot find workload' in l:            return []
        lines.append(l.replace('\n',''))

    #avoid writing godzillion lines files
    #f=open(request+".workload",'w')
    #f.write('\n'.join(lines))
    #f.close()
    
    return lines

def TransformRequestIntoDict( request, allowed=None, onlyRequest=False, inObject=False ):
    final={}
    for line in getlines( request ):
        if not line: continue
        towhich=final
        #print line
        if not '=' in line: continue
        try:
            (fields,value)=line.split('=',1)
        except:
            print line
            print "does not comply"
            continue
                    
        #print fields,value
        fieldsL=map(lambda s : s.strip(), fields.split('.'))
        #skip 0 because it's the request name ?
        (fields,leaf)=(fieldsL[0:-1],fieldsL[-1].rstrip())
        if 'pickl' in leaf: continue
        if allowed:
            go=False
            #print fields[1]
            #if any( map(lambda f : f in allowed, fieldsL)):
            #    go=True
            for allow in allowed:
                if type(allow)==str:
                    if allow in fieldsL:
                        go=True
                        break
                elif type(allow)==list:
                    if all(map(lambda a: a in fieldsL, allow)):
                        go=True
                        break
                        
            if not go:
                continue

        for field in fields:
            if not field in towhich:
                towhich[field]={}
            towhich = towhich[field]
            #print "adding.",field
        # assign value
        try:
            towhich[leaf]= eval(value.lstrip())
        #towhich[leaf]= json.loads(value.lstrip())
        except:
            try:
                towhich[leaf]= eval(value.lstrip().lstrip("'").rstrip("'"))
            except:
                print "cannot convert"
                print leaf
                #print value.lstrip()
                print value
                raise Exception(traceback.format_exc())

    if final=={}:
        return None
    #pprint.pprint(final)
    #pprint.pprint(final[request]['request'])
    if onlyRequest:
        if inObject:
            return dict2obj(final[request])
        else:
            return final[request]
    else:
        if inObject:
            return dict2obj(final)
        else:
            return final


#TransformRequestIntoDict( 'spinoso_TOP-Summer12-00074_R1315_B80_01_LHE_120517_152452_1772' )
