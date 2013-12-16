def dict2obj(d,andList=False):
    class C(object):
        def __str__(self):
            return self.rep
        pass
    if isinstance(d, dict):
        o = C()
        for k in d:
            o.__dict__[k] = dict2obj(d[k],andList)
        import pprint
        o.__dict__['rep']=pprint.pformat(d)
        return o
    elif isinstance(d, list) and andList:
        l=[]
        for k in d:
            l.append( dict2obj(k,andList) )
        return l
    else:
        return d
