import os,httplib,pprint,json
from dict2obj import dict2obj

def phedex(dataset,verbose=False):


    def getone(urlconn,url):
        conn = httplib.HTTPSConnection(urlconn,
                                       cert_file = os.getenv('X509_USER_PROXY'),
                                       key_file = os.getenv('X509_USER_PROXY')
                                       )
        print "phedex query",url
        conn.request('GET',url)
        response = conn.getresponse()

        try:
            phedex=json.load(response)
            if verbose:
                print "raw output"
                pprint.pprint(phedex)
        except:
            print "##failed phedex read for "+dataset
            return {}
        return phedex['phedex']

    if dataset =='None Yet' or dataset =='?':
        return None
    
    url="/phedex/datasvc/json/prod/blockreplicas?dataset="+dataset
    urlconn='cmsweb.cern.ch'

    phedex={}
    phedex.update(getone('cmsweb.cern.ch','/phedex/datasvc/json/prod/blockreplicas?dataset='+dataset))
    phedex.update(getone('cmsweb.cern.ch','/phedex/datasvc/json/prod/requestlist?dataset='+dataset))
    
    if len(phedex)==0:
        return None
    #phedex=phedex['phedex']
    ph=dict2obj(phedex,andList=True)
    return ph

def transferToCustodial(ph):
    allcomplete=[]
    allincomplete=[]
    complete=[]
    incomplete=[]
    nBlock=len(ph.block)
    for b in ph.block:
        #pprint.pprint(b)
        dataset=b.name.split('#')[0]
        for replica in b.replica:
            if replica.complete=='y':
                allcomplete.append(replica.node)
                #print b.name,replica.node
            else:
                allincomplete.append(replica.node)
            if replica.custodial=='y':
                if replica.complete=='y':
                    complete.append(b.name)
                else:
                    incomplete.append(b.name)
    complete=list(set(complete))
    incomplete=list(set(incomplete))
    allcomplete=list(set(allcomplete))
    allincomplete=list(set(allincomplete))

    #print pprint.pformat(allcomplete),'->',filter(lambda s : 'T1' in s,  allincomplete)
    #print pprint.pformat(allcomplete),'->',filter(lambda s : 'T2' in s,  allincomplete)
    #print "going to",len(filter(lambda s : 'T1' in s,  allincomplete)),"T1\n",pprint.pformat(filter(lambda s : 'T1' in s,  allincomplete))
    #print "going to",len(filter(lambda s : 'T2' in s,  allincomplete)),"T2\n",pprint.pformat(filter(lambda s : 'T2' in s,  allincomplete))
    #print "living on",len(filter(lambda s : 'T2' in s,  allcomplete)),"T2\n",pprint.pformat(filter(lambda s : 'T2' in s,  allcomplete))
    
    return (complete,incomplete,allcomplete,allincomplete)


def atT3(ph):
    if not ph: return []
    where=[]
    if not hasattr(ph,'request'):
        print "No requests for",ph
        return where
    for req in ph.request:
        if req.approval == 'approved':
            for node in req.node:
                if node.name.startswith('T3'):
                    where.append(node.name)
                    
    where=list(set(where))
    return where

def atT3DS(dataset):
    ph=phedex(dataset)
    return atT3(ph)

def atAnyT3(ph):
    return len(atT3(ph))!=0

def atT2DS(dataset):
    ph=phedex(dataset)
    return atT2(ph)

def atT2(ph):    
    if not ph:
        return []
    (complete,incomplete,allcomplete,allincomplete) = transferToCustodial(ph)
    anyT2=filter(lambda s : 'T2' in s,  allcomplete)
    gAnyT2=filter(lambda s : 'T2' in s,  allincomplete)
    return anyT2+gAnyT2
    
def atAnyT2(ph):
    (complete,incomplete,allcomplete,allincomplete) = transferToCustodial(ph)
    anyT2=filter(lambda s : 'T2' in s,  allcomplete)
    gAnyT2=filter(lambda s : 'T2' in s,  allincomplete)
    print "living on",len(anyT2),"T2\n",pprint.pformat(anyT2)

    return len(anyT2)!=0
    
def atCustodial(ph):
    (complete,incomplete,allc,allin) = transferToCustodial(ph)
    if len(complete)==0:
        return (False,"0%")
    else:
        if len(incomplete)!=0:
            return (False,"%d%%"%( len(complete) / len(complete+incomplete) ))
        else:
            return (True,"100%")
        
def custodialDS(dataset):
    ph=phedex(dataset)
    return custodials(ph)

def custodials(ph):
    sites=[]
    if not ph : return sites
    dataset=None
    if not hasattr(ph,'block'):
        print "No blocks for",ph
        return sites
    for b in ph.block:
        #pprint.pprint(b)
        dataset=b.name.split('#')[0]
        for replica in b.replica:
            if replica.custodial=='y':
                #print replica
                sites.append(str(replica.node))


    sites=list(set(sites))
    #print "sites",sites,"for",dataset
    return sites

def openBlocks(ph):
    if not ph: return []
    blocks=[]
    blocks=filter( lambda b : b.is_open=='y', ph.block)
    #pprint.pprint(blocks)
    return blocks

def runningSitesForDS(dataset):
    ph=phedex(dataset)
    return runningSites(ph)

def runningSites(ph):
    opl=openBlocks(ph)
    sites=[]
    if not ph: return sites
    for rpl in [b.replica for b in opl]:
        for replica in rpl:
            sites.append(str(replica.node))
    sites=list(set(sites))
    #print "Sites where blocks are open\n",sites
    return sites
