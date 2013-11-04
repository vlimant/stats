
export DBS_CLIENT_CONFIG="/afs/cern.ch/cms/slc5_amd64_gcc462/cms/dbs-client/DBS_2_1_1_patch1_1/lib/DBSAPI/dbs.config"
ps aux | grep python2.6 | grep -v grep | awk -F\   '{print $2;}' | head -n 1 | xargs -IIII sh -c "kill -9 'III'"; nohup python2.6 main.py &>acmw16.log &
