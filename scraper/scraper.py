#!/usr/bin/env python
import os
import json
import pandas as pd
import sqlite3 as lite
from subprocess import Popen, PIPE
from collections import Counter

sql_file = os.environ['JOB_SQL_PATH']

def add_refdata(energy,**args):
    '''
    Writes an entry to the sql table
    '''
    cov = '{}x{}'.format(*args['cov'])
    data = [args['host'],args['fac'],args['ads'],args['site'],cov,
            energy,args['root']]
    con = lite.connect(sql_file)
    cur = con.cursor()
    try:
        cur.execute("INSERT into refdata VALUES(?,?,?,?,?,?,?)",((data)))
    except lite.Error, e:
        print "Error %s:" % e.args[0]
        sys.exit(1)
    con.commit()
    con.close()

def job_data():
    '''
    Module to scrap job/user data from squeue/sacct output
    '''
    jobq = Popen(['squeue', '-M', 'smp'], stdout=PIPE, stderr=PIPE)
    out, error = jobq.communicate()
    
    all_jobs = out.strip().split('\n')
    all_states, all_reasons, users = [], [], []
    
    for jline in all_jobs[-2:]:
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
    for gr in g.groups:
        print gr, len(g.groups[gr])

    jobq = Popen(['sacct', '-j', jid], stdout=PIPE, stderr=PIPE)
    out, error = jobq.communicate()
    print out
        
    print Counter(all_states) , Counter(all_reasons)  

def usage_status():
    '''
    Module to scrap node availability status from sinfo 
    '''
    sinfo = Popen(['sinfo', '-M', 'smp'], stdout=PIPE, stderr=PIPE)
    out, error = sinfo.communicate()
    
    all_parts = out.strip().split('\n')
    for parts in all_parts:
        pss = parts.split()
        if pss[0].startswith('smp'):
            print pss
