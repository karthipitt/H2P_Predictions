#!/usr/bin/python

import os,sys
import json
import pandas as pd
import sqlite3 as lite
from datetime import datetime
from subprocess import Popen, PIPE
from collections import Counter
from apscheduler.schedulers.blocking import BlockingScheduler

sql_file = os.environ['H2P_DATA_PATH']

def clus_data():
    '''
    Get usage info from sinfo for all clusters and partitions
    Write data to clusdata table
    ''' 

    ts = datetime.now()
    con = lite.connect(sql_file)
    cur = con.cursor()
    clus_dict = {'mpi':['opa'], 'smp':['smp','high-mem']}
    qdinfo = {}
    for clus, partition in clus_dict.iteritems():
        for p in partition:
            nodeinfo = Popen(['sinfo', '-M', clus, '-p', p, '-O', 'nodeaiot'], stdout=PIPE, stderr=PIPE)
            out, error = nodeinfo.communicate()
            nodestat = out.strip().split('\n')[-1].split('/')
            nodestat = list(map(lambda x: int(x), nodestat))

            cpuinfo = Popen(['sinfo', '-M', clus, '-p', p, '-O', 'cpusstate'], stdout=PIPE, stderr=PIPE)
            out, error = cpuinfo.communicate()
            cpustat = out.strip().split('\n')[-1].split('/')
            cpustat = list(map(lambda x: int(x), cpustat))
            state, reason, g = job_data(clus,p,ts)
            qdinfo['{} {}'.format(clus,p)] = g
            data = [ts, clus, p, state['R'], state['PD']]
            data = data + nodestat + cpustat
            holders = ','.join('?' * 13)
            try:
                cur.execute("INSERT into clusdata VALUES({})".format(holders),((data)))
            except lite.Error, e:
                print "Error %s:" % e.args[0]
                sys.exit(1)
    con.commit()
    con.close()
    add_data(qdinfo, ts) 

def job_data(clus, p, ts):
    '''
    Get 'PD' job info for all clusters and partitions
    Groups PD jobs by user/reason for PD
    '''

    jobq = Popen(['squeue', '-M', clus, '-p', p], stdout=PIPE, stderr=PIPE)
    out, error = jobq.communicate()
    
    all_jobs = out.strip().split('\n')
    all_states, all_reasons, users = [], [], []
    
    for jline in all_jobs[2:]:
        jsplit = jline.split()
        jid = jsplit[0]
        state = jsplit[4]
        reason = jsplit[7]
        all_states.append(state)
        if reason.startswith('('):
            all_reasons.append(reason[1:-1])
            users.append(jsplit[3])
            
    d = {'user': pd.Series(users),
         'reason':pd.Series(all_reasons)}
    df = pd.DataFrame(d)
    g = df.groupby(['user','reason'])
    return Counter(all_states), Counter(all_reasons), g 

def add_data(qdinfo, ts):
    '''
    Writes data to qdjobinfo table
    '''

    con = lite.connect(sql_file)
    cur = con.cursor()
    for q,g in qdinfo.iteritems():
        clus = q.split()[0]
        p = q.split()[1]
        for gr in g.groups:
            try:
                data = [ts, clus, p, gr[0], gr[1], len(g.groups[gr])]
                cur.execute("INSERT into qdjobinfo VALUES(?,?,?,?,?,?)",((data)))
            except lite.Error, e:
                print "Error %s:" % e.args[0]
                sys.exit(1)
    con.commit()
    con.close()

if __name__ == '__main__':
    scheduler = BlockingScheduler()
    scheduler.add_job(clus_data, 'interval', seconds=600)
#    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))
    
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass

