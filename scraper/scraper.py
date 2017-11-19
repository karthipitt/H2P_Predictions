#!/usr/bin/env python
import json
import os
from subprocess import Popen, PIPE
from collections import Counter
import pandas as pd

data = json.load(open('accountinfo.json'))

jobq = Popen(['squeue', '-M', 'smp'], stdout=PIPE, stderr=PIPE)
out, error = jobq.communicate()

all_jobs = out.strip().split('\n')
jobids = [ '917746',' 917745', '918134']
all_states, all_reasons = [], []
users = []

for j in all_jobs[2:]:
#for j in jobids:
    jsplit = j.split()
    jid = jsplit[0]
    state = jsplit[4]
    all_states.append(state)
    reason = jsplit[7]
    if reason.startswith('('):
        all_reasons.append(reason[1:-1])
        users.append(jsplit[3])
        
    #jobq = Popen(['sacct', '-j', jid], stdout=PIPE, stderr=PIPE)
    #out, error = jobq.communicate()
    #print out
    
print Counter(all_states) , Counter(all_reasons)  
#d = {'user': pd.Series(users),
#     'reason':pd.Series(all_reasons)}
#df = pd.DataFrame(d)
#g = df.groupby(['user','reason'])
#for gr in g.groups:
#    print gr, len(g.groups[gr]),
#    for k, v in data.iteritems():
#        if gr[0] in v:
#            print k
#

sinfo = Popen(['sinfo', '-M', 'smp'], stdout=PIPE, stderr=PIPE)
out, error = sinfo.communicate()

all_parts = out.strip().split('\n')
for parts in all_parts:
    pss = parts.split()
    if pss[0].startswith('smp'):
        print pss
