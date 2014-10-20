from couchDB import Interface
import json
import pprint
import ROOT
import copy
import time
import os

def plotGrowth(thisDoc,i,force=False,wait=False):
    one=thisDoc['pdmv_request_name']
    ##triple check
    ##really only change the plot for running requests
    if not thisDoc['pdmv_status_from_reqmngr'].startswith('running') and not thisDoc['pdmv_status_from_reqmngr'] == 'completed' and not force:
        return
    
    #today=time.mktime(time.gmtime())    
    today=time.mktime(time.strptime(time.asctime()))
    
    c=ROOT.TCanvas('c','c',0,0,1000,400)
    c.SetGrid()    
    gr=ROOT.TGraph()
    grc=ROOT.TGraph()
    graphs_by_ds = {}

    gr.SetMarkerStyle(5)#7 small dots
    gr.SetLineWidth(4)
    grc.SetLineWidth(4)
    grc.SetMarkerStyle(5)#7 small dots
    grc.SetLineColor(4)
    
    grn=0
    Nunit=1000000.
        
    gr.SetTitle(thisDoc['pdmv_prep_id']+' '+thisDoc['pdmv_dataset_name'])
    Nexpected=float(thisDoc['pdmv_expected_events'])
    Nexpected/=Nunit
    
    print "plotting",one
    maxYaxis=Nexpected
    
    if 'pdmv_monitor_history' in thisDoc:
        print "getting history from history"

        canStopNext=False
        earliest=0
        for nextOne in thisDoc['pdmv_monitor_history']:
            up=  time.mktime(time.strptime(nextOne['pdmv_monitor_time'])) - today
            up/= 60.*60.*24.*7.

            if 'pdmv_dataset_statuses' in nextOne:
                #print "getting",nextOne['pdmv_dataset_statuses'].keys()
                for ds in nextOne['pdmv_dataset_statuses'].keys():
                    if ds == thisDoc['pdmv_dataset_name']: continue ## already taken care of        
                    if not ds in graphs_by_ds.keys():
                        #print "added a graph for",ds
                        graphs_by_ds[ds] = ROOT.TGraph()
                        graphs_by_ds[ds].SetName(ds.replace('/',''))
                        graphs_by_ds[ds].SetLineWidth(4)
                        graphs_by_ds[ds].SetLineColor(3+len(graphs_by_ds.keys()))
                        graphs_by_ds[ds].SetMarkerStyle(5)

                    oN = nextOne['pdmv_dataset_statuses'][ds]['pdmv_evts_in_DAS']
                    #if oN<1: continue
                    oN /= Nunit
                    index=graphs_by_ds[ds].GetN()
                    graphs_by_ds[ds].SetPoint(index, up, oN)
                    #print "added entry",index,"at", up, oN,"for",ds
                    

            N=nextOne['pdmv_evts_in_DAS'] + nextOne['pdmv_open_evts_in_DAS']
            #do not put too many zero points
            #if N<1: continue


            N/=Nunit
            gr.SetPoint(grn,up,N)
            grc.SetPoint(grn,up,nextOne['pdmv_evts_in_DAS']/Nunit)
            grn+=1
        
            if maxYaxis<N:
                maxYaxis=N
            if earliest>up:
                earliest=up
            
                


    else:
        print "getting the plot from revisions"
        revs=thisDoc['_revs_info']
        revs.reverse()
        canStopNext=False
        earliest=0
        for rev in revs:
            try:
                nextOne=i.get_file_info_rev(one,rev['rev'])
            except:
                continue
            
            if nextOne['pdmv_status_from_reqmngr']=='announced':
                canStopNext=True
            if canStopNext:
                continue
        
            up=  time.mktime(time.strptime(nextOne['pdmv_monitor_time'])) - today
            up/= 60.*60.*24.*7.
            
            N=nextOne['pdmv_evts_in_DAS'] + nextOne['pdmv_open_evts_in_DAS']
            #do not put too many zero points
            if N<1: continue


            N/=Nunit
            gr.SetPoint(grn,up,N)
            grc.SetPoint(grn,up,nextOne['pdmv_evts_in_DAS']/Nunit)
            grn+=1
        
            if maxYaxis<N:
                maxYaxis=N
            if earliest>up:
                earliest=up

    if grn!=0:
        
        ##only plot those with something in
        expecteds=[]
        if graphs_by_ds:
            mg = ROOT.TMultiGraph()
            if thisDoc['pdmv_prep_id'].strip() not in ['','No-Prepid-Found']:
                mg.SetTitle("Evolution graph for %s"%( thisDoc['pdmv_prep_id'] ))
            else:
                mg.SetTitle("Evolution graph for %s"%( thisDoc['pdmv_request_name'] ))
            mg.Add( gr ,"lp" )
            mg.Add( grc, "lp" )
            for (ds,g) in graphs_by_ds.items():
                mg.Add( g, "lp")
            mg.Draw("a")
            mg.GetXaxis().SetTitle('Weeks before today'+time.ctime(today))
            mg.GetYaxis().SetTitle('M events')
            
            for (ds,g) in graphs_by_ds.items():
                if 'pdmv_expected_events_per_ds' in thisDoc and ds in thisDoc['pdmv_expected_events_per_ds']:
                    expecteds.append( ROOT.TLine(mg.GetXaxis().GetXmin(), thisDoc['pdmv_expected_events_per_ds'][ds]/Nunit,
                                                 mg.GetXaxis().GetXmax(), thisDoc['pdmv_expected_events_per_ds'][ds]/Nunit)
                                      )
                    expecteds[-1].SetLineStyle(g.GetLineColor() ) ## tie the style to the color
                    expecteds[-1].SetLineWidth(4)
                    expecteds[-1].SetLineColor( g.GetLineColor() )
                    expecteds[-1].Draw()
            
        else:
            gr.Draw('apl')
            grc.Draw('samepl')
            gr.GetXaxis().SetTitle('Weeks before today'+time.ctime(today))
            gr.GetYaxis().SetTitle('M events')
            #gr.GetYaxis().SetRangeUser(0,gr.GetYaxis().GetXmax())
            gr.GetYaxis().SetRangeUser(0,maxYaxis*1.05)
            gr.GetXaxis().SetRangeUser(gr.GetXaxis().GetXmin(),0.1)
        
        if not expecteds:
            expected=ROOT.TLine(gr.GetXaxis().GetXmin(),Nexpected,
                                gr.GetXaxis().GetXmax(),Nexpected)
            expected.SetLineWidth(4)
            expected.SetLineColor(2)
            expected.Draw()
    

        dir,file = one.rsplit('_', 1)
        dir = os.path.normpath(dir.replace('_', '/'))
        os.system('mkdir -p /afs/cern.ch/cms/PPD/PdmV/web/stats/growth/%s ' % (dir))
        c.Print('/afs/cern.ch/cms/PPD/PdmV/web/stats/growth/%s/%s.gif' % (dir, file))
        #c.Print('/afs/cern.ch/cms/PPD/PdmV/web/stats/growth/'+one+'.gif')

        if wait:
            extrap=ROOT.TF1('extrap','[0]*x+[1]',earliest*0.25,0.01)
            gr.Fit('extrap',"R","")
            #extrap.SetRange()
            extrap.Draw("same")
            a=extrap.GetParameter(0)
            b=extrap.GetParameter(1)
            if a!=0:
                eta=(Nexpected-a)/b
                print "ETA is:",eta,"weeks",eta*7.,"days"
            else:
                print "cannot compute eta"
            while True:
                time.sleep(1)
    else:
        print one,"has still no entries"

if __name__ == "__main__":
    i=Interface('http://cms-pdmv-golem.cern.ch:5984/stats')

    allDocs=i.get_all_files()
    docs = [doc['id'] for doc in allDocs['rows']]

    test='vlimant_Winter532012BMuOniaParked_ASGCPrio1_537p5_130122_195107_1235'
    test='nnazirid_BPH-Summer12_DR53X-00091_T1_FR_CCIN2P3_MSS_000_v1__130121_143546_6312'
    #test='nnazirid_HIG-Summer12_DR53X-00956_T1_FR_CCIN2P3_MSS_000_v1__130121_153018_5646'
    
    for one in docs:
        if one !=test: continue
        
        thisDoc=i.get_file_info_withrev(one)
        plotGrowth(thisDoc,i,wait=True)
    
