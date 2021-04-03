# -*- coding: utf-8 -*-

import sys

jobname = sys.argv[1]
input_file = '/home/samza/GroundTruth/nexmark_result/' + jobname + "/000001.txt"
# input_file = 'GroundTruth/stdout'
output_path = 'figures/' + jobname + '/'
xaxes = [0, 1955]#[0, 1355]#[0000, 755]
deltaT = 100
startline = [[155, 155], [0, 10000000]]
executorsFigureFlag = False

colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k']
lineType = ['--', '-.', ':']
markerType = ['+', 'o', 'd', '*', 'x']

schema = ['Time', 'Container Id', 'Arrival Rate', 'Service Rate', 'Window Estimate Delay']
containerArrivalRate = {}
containerServiceRate = {}
containerWindowDelay = {}
containerRealWindowDelay = {}
containerResidual = {}
containerArrivalRateT = {}
containerServiceRateT = {}  
containerLongtermDelay = {}
containerLongtermDelayT = {}
containerWindowDelayT = {}
containerRealWindowDelayT = {}
containerResidualT = {}
overallWindowDelay = []
overallRealWindowDelay = []
overallWindowDelayT = []
overallRealWindowDelayT = []

numberOfOEs = []
numberOfOEsT = []
decisionT = []
decision = []
numberOfSevere = []
numberOfSevereT = []

totalArrivalRate = {}
userWindowSize = 2000

migrationDecisionTime = {}
migrationDeployTime = {}

scalingDecisionTime = {}
scalingDeployTime = {}
ret = []

initialTime = -1
def parseContainerArrivalRate(split, base):
    global initialTime
    time = split[2]
    if(initialTime == -1):
        initialTime = long(time)
    info = "".join(split[6:]).replace(' ','')
    info = info.replace('{','')
    info = info.replace('}','')
    containers = info.split(',')
    total = 0
    for container in containers:
        if(len(container)>0):
            Id = container.split('=')[0]
            value = container.split('=')[1]
            if(value == 'NaN'): value = '0'
            total += float(value) * 1000
            if(Id not in containerArrivalRate):
                containerArrivalRate[Id] = []
                containerArrivalRateT[Id] = []
            containerArrivalRate[Id] += [float(value)* 1000]
            containerArrivalRateT[Id] += [(long(time) - initialTime)/base]
            if( (long(time) - initialTime)/base not in totalArrivalRate):
                totalArrivalRate[(long(time) - initialTime)/base] = 0.0
            totalArrivalRate[(long(time) - initialTime)/base] += float(value) * 1000

def parseContainerServiceRate(split, base):
    global initialTime
    time = split[2]
    if(initialTime == -1):
        initialTime = long(time)
    info = "".join(split[6:]).replace(' ','')
    info = info.replace('{','')
    info = info.replace('}','')
    containers = info.split(',')
    total = 0
    for container in containers:
        if(len(container)>0):
            Id = container.split('=')[0]
            value = container.split('=')[1]
            if(value == 'NaN'): value = '0'
            total += float(value) * 1000
            if(Id not in containerServiceRate):
                containerServiceRate[Id] = []
                containerServiceRateT[Id] = []
            containerServiceRate[Id] += [float(value)* 1000]
            containerServiceRateT[Id] += [(long(time) - initialTime)/base]

def parseContainerWindowDelay(split, base):
    global initialTime, overallWindowDelayT, overallWindowDelay
    time = split[2]
    if(initialTime == -1):
        initialTime = long(time)
    info = "".join(split[6:]).replace(' ','')
    info = info.replace('{','')
    info = info.replace('}','')
    containers = info.split(',')
    total = 0
    for container in containers:
        if(len(container)>0):
            Id = container.split('=')[0]
            value = container.split('=')[1]
            total += float(value)
            if(Id not in containerWindowDelay):
                containerWindowDelay[Id] = []
                containerWindowDelayT[Id] = []
            containerWindowDelay[Id] += [float(value)]
            containerWindowDelayT[Id] += [(long(time) - initialTime)/base]

            if(len(overallWindowDelayT) == 0 or overallWindowDelayT[-1] < (long(time) - initialTime)/base):
                overallWindowDelayT += [(long(time) - initialTime)/base]
                overallWindowDelay += [float(value)]
            elif(overallWindowDelay[-1] < float(value)):
                overallWindowDelay[-1] = float(value)

def parseContainerLongtermDelay(split, base):
    global initialTime
    time = split[2]
    if(initialTime == -1):
        initialTime = long(time)
    info = "".join(split[6:]).replace(' ','')
    info = info.replace('{','')
    info = info.replace('}','')
    containers = info.split(',')
    total = 0
    for container in containers:
        if(len(container)>0):
            Id = container.split('=')[0]
            value = container.split('=')[1]
            total += float(value)
            if(Id not in containerLongtermDelay):
                containerLongtermDelay[Id] = []
                containerLongtermDelayT[Id] = []
            value = float(value)
            if(value>1000000): value = 1000000
            elif(value<0): value = 0
            containerLongtermDelay[Id] += [float(value)]
            containerLongtermDelayT[Id] += [(long(time) - initialTime)/base]

def readContainerRealWindowDelay(Id):
    global initialTime, overallWindowDelayT, overallWindowDelay, overallRealWindowDelayT, overallRealWindowDelay, userWindowSize
    fileName = "container" + Id + ".txt"
    counter = 1
    processed = 0
    size = 0
    base = 1    
    queue = []
    total = 0;
    lastTime = -100000000
    with open(fileName) as f:
        lines = f.readlines()
        for i in range(0, len(lines)):
            line = lines[i]
            split = line.rstrip().split(' ')
            if(split[0] == 'Partition'):
                split = split[1].split(',')
                partition = split[0]
                processed += 1
                time = split[2]
                time = (long(time) - initialTime)/base
                queue += [[time, float(split[1])]]
                total += float(split[1])
                while(queue[0][0] < time - userWindowSize):
                    total -= queue[0][1]
                    queue = queue[1:]
            
                
                if(lastTime <= time - 400 and len(queue) > 0):
                    if(Id not in containerRealWindowDelay):
                        containerRealWindowDelayT[Id] = []
                        containerRealWindowDelay[Id] = []
                    containerRealWindowDelayT[Id] += [time]
                    containerRealWindowDelay[Id] += [total/len(queue)]

                    
print("Reading from file:" + input_file)
counter = 0
arrived = {}

base = 1
with open(input_file) as f:
    lines = f.readlines()
    for i in range(0, len(lines)):
        line = lines[i]
        split = line.rstrip().split(' ')
        counter += 1
        if(counter % 100 == 0):
            print("Processed to line:" + str(counter))

        if ((split[0] == 'DelayEstimateModel,' or split[0] == 'Model,') and split[4] == 'Arrival' and split[5] == 'Rate:'):
            parseContainerArrivalRate(split, base)
        if ((split[0] == 'DelayEstimateModel,' or split[0] == 'Model,') and split[4] == 'Service' and split[5] == 'Rate:'):
            parseContainerServiceRate(split, base)
        if ((split[0] == 'DelayEstimateModel,' or split[0] == 'Model,') and split[4] == 'Instantaneous' and split[5] == 'Delay:'):
            parseContainerWindowDelay(split, base)
        if ((split[0] == 'DelayEstimateModel,' or split[0] == 'Model,') and split[4] == 'Longterm' and split[5] == 'Delay:'):
            parseContainerLongtermDelay(split, base)

        #Add migration marker
        if (split[0] == 'Migration!'):
            j = split.index("time:")
            if (not split[j + 4].startswith('[')):
                src = split[j + 4]
                time = (long(split[j + 1]) - initialTime) / base
                tgt = split[j + 7].rstrip()
                if (src not in migrationDecisionTime):
                    migrationDecisionTime[src] = []
                migrationDecisionTime[src] += [time]
                if (tgt not in migrationDecisionTime):
                    migrationDecisionTime[tgt] = []
                migrationDecisionTime[tgt] += [-time]
                print(str(src) + ' !!! ' + str(tgt))
            else:  # Multi source and target
                mid = split[j + 4:].index('to')
                srcs = [split[tt].rstrip().replace('[', '').replace(']', '').replace(',', '') for tt in
                        range(j + 4, j + 4 + mid)]
                tgts = [split[tt].rstrip().replace('[', '').replace(']', '').replace(',', '') for tt in
                        range(j + 4 + mid + 2, len(split))]
                time = (long(split[j + 1]) - initialTime) / base
                for t in srcs:
                    if (t not in migrationDecisionTime):
                        migrationDecisionTime[t] = []
                    migrationDecisionTime[t] += [time]
                for t in tgts:
                    if (t not in migrationDecisionTime):
                        migrationDecisionTime[t] = []
                    migrationDecisionTime[t] += [-time]
                print(str(srcs) + ' !!! ' + str(tgts))

            decisionT += [time]
            if(split[1] == 'Scale' and split[2] == 'in'):
                decision += [-1]
            elif(split[1] == 'Scale' and split[2] == 'out'):
                decision += [1]
            else:
                decision += [0]

        if (split[0] == 'Number' and split[2] == 'severe'):
            time = int(lines[i-1].split(' ')[2])
            numberOfSevereT += [time]
            numberOfSevere += [int(split[4])]

        if (split[0] == 'Executors' and split[1] == 'stopped'):
            i = split.index('from')
            if (not split[i + 1].startswith('[')):
                src = split[i + 1]
                tgt = split[i + 3].rstrip()
                print('Migration complete from ' + src + ' to ' + tgt)
                time = (long(split[4]) - initialTime) / base
                if (src not in migrationDeployTime):
                    migrationDeployTime[src] = []
                migrationDeployTime[src] += [time]
                if (tgt not in migrationDeployTime):
                    migrationDeployTime[tgt] = []
                migrationDeployTime[tgt] += [-time]
                if (len(numberOfOEs) == 0):
                    numberOfOEs += [len(containerArrivalRate)]
                    numberOfOEsT += [0]
                if (split[6] == 'scale-in'):
                    numberOfOEs += [numberOfOEs[-1]]
                    numberOfOEsT += [time]
                    numberOfOEs += [numberOfOEs[-1] - 1]
                    numberOfOEsT += [time]
                if (split[6] == 'scale-out'):
                    numberOfOEs += [numberOfOEs[-1]]
                    numberOfOEsT += [time]
                    numberOfOEs += [numberOfOEs[-1] + 1]
                    numberOfOEsT += [time]
            else:  # multi source and target
                mid = split[i + 1:].index('to')
                if (',new' not in split):
                    tgts = [split[tt].rstrip().replace('[', '').replace(']', '').replace(',', '') for tt in
                            range(i + 1 + mid + 1, len(split))]
                else:  # Scale-out mix load-balance
                    tmid = split[mid + 1:].index(',new')
                    tgts = [split[tt].rstrip().replace('[', '').replace(']', '').replace(',', '') for tt in
                            range(mid + 1 + tmid + 1, len(split))]
                    print('tgt=' + str(tgts))
                srcs = [split[tt].rstrip().replace('[', '').replace(']', '').replace(',', '') for tt in
                        range(i + 1, i + 1 + mid)]
                print('Migration complete from ' + str(srcs) + ' to ' + str(tgts))
                time = (long(split[4]) - initialTime) / base
                for t in srcs:
                    if (t not in migrationDeployTime):
                        migrationDeployTime[t] = []
                    migrationDeployTime[t] += [time]
                for t in tgts:
                    if (t not in migrationDeployTime):
                        migrationDeployTime[t] = []
                    migrationDeployTime[t] += [-time]
                if (len(numberOfOEs) == 0):
                    numberOfOEs += [len(containerArrivalRate)]
                    numberOfOEsT += [0]
                if (split[6] == 'scale-in'):
                    numberOfOEs += [numberOfOEs[-1]]
                    numberOfOEsT += [time]
                    numberOfOEs += [numberOfOEs[-1] - len(srcs)]
                    numberOfOEsT += [time]
                if (split[6] == 'scale-out'):
                    numberOfOEs += [numberOfOEs[-1]]
                    numberOfOEsT += [time]
                    numberOfOEs += [numberOfOEs[-1] + len(tgts)]
                    numberOfOEsT += [time]

import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

def addMigrationLine(Id, ly):
    lines = []
    if(Id not in migrationDecisionTime):
        return lines
    for i in range(len(migrationDecisionTime[Id])):
        if(migrationDecisionTime[Id][i] > 0): #Migrate out
            X = [migrationDecisionTime[Id][i]]
            X += [migrationDecisionTime[Id][i]]
            Y = [0]
            Y += [ly]
            lines += [[X, Y, 'g']]
        else:
            X = [-migrationDecisionTime[Id][i]]
            X += [-migrationDecisionTime[Id][i]]
            Y = [0]
            Y += [ly]
            lines += [[X, Y, 'y']]
    if(Id not in migrationDeployTime):
        return lines
    for i in range(len(migrationDeployTime[Id])):
        if(migrationDeployTime[Id][i] > 0): #Migrate out
            X = [migrationDeployTime[Id][i]]
            X += [migrationDeployTime[Id][i]]
            Y = [0]
            Y += [ly]
            lines += [[X, Y, 'b']]
        else:
            X = [-migrationDeployTime[Id][i]]
            X += [-migrationDeployTime[Id][i]]
            Y = [0]
            Y += [ly]
            lines += [[X, Y, 'r']]
          
    return lines

from ViolationIntervalFigure import getViolationPerTimeSlot
violationTimeslot = getViolationPerTimeSlot(jobname)

# Number of OEs
print("Draw # of OEs")
if(len(numberOfOEs) == 0):
    numberOfOEs += [len(containerArrivalRate)]
    numberOfOEsT += [0]
lastTime = 1
for Id in containerArrivalRateT:
    if(lastTime < containerArrivalRateT[Id][-1]):
        lastTime = containerArrivalRateT[Id][-1]
numberOfOEs += [numberOfOEs[-1]]
numberOfOEsT += [lastTime]
#print(numberOfOEsT)
#print(numberOfOEs)


legend = ['Number of Executor']
fig = plt.figure(figsize=(60,30))
numberOfOEsT = [i * deltaT / 1000.0 for i in numberOfOEsT]
plt.plot(numberOfOEsT, numberOfOEs, 'b')
plt.plot(startline[0], startline[1], 'r')
#Violations
plt.plot([x + 35 for x in violationTimeslot.keys()], violationTimeslot.values(), 'go', markersize=5)

#print(decisionT)
#print(numberOfOEsT)
# Add decision marker
decisionT = [i *deltaT / 1000.0 for i in decisionT]
oeIndex = 0
for i in range(0, len(decisionT)):
    while (oeIndex < len(numberOfOEsT) and numberOfOEsT[oeIndex] < decisionT[i]):
        oeIndex += 1
    markerX = decisionT[i]
    if(oeIndex < len(numberOfOEsT)):
        markerY = numberOfOEs[oeIndex]
    else:
        markerY = numberOfOEs[oeIndex - 1]
    if(decision[i] == 0):
        markerLabel = 'y|'
    elif(decision[i] == 1):
        markerLabel = 'r|'
    else:
        markerLabel = 'g|'
    plt.plot(markerX, markerY, markerLabel, ms=30)
 
# Add severe number
for i in range(0, len(numberOfSevereT)):
    x = numberOfSevereT[i] *deltaT / 1000.0
    y = numberOfSevere[i]
    plt.plot([x, x + 1], [y, y], 'r')
    plt.plot([x, x], [y, 0], 'r')
    plt.plot([x + 1, x + 1], [y, 0], 'r')
plt.legend(legend, loc='upper left')
plt.grid(True)
axes = plt.gca()
maxOEs = 65
axes.set_yticks(np.arange(0, maxOEs))
axes.set_xticks(np.arange(0, xaxes[1], 20 ))#755, 20))
axes.set_xlim(xaxes)
axes.set_ylim([0,maxOEs + 1])
plt.xlabel('Index (s)')
plt.ylabel('# of Running OEs')
plt.title('Number of OEs')
plt.savefig(output_path + jobname + '_NumberOfOEwithV.png')
plt.close(fig)

    #Draw total arrival rate
print("Draw total arrival rate")
arrivalRate = []
arrivalRateT = []
for x in sorted(totalArrivalRate):
    arrivalRateT += [x]
    arrivalRate += [totalArrivalRate[x]]

legend = ['Arrival Rate']
fig = plt.figure(figsize=(60,40))
ax = fig.add_subplot(111)
arrivalRateT = [i * deltaT / 1000.0 for i in arrivalRateT]
ax.plot(arrivalRateT, arrivalRate , 'b^', markersize=1)
ax.plot(startline[0], startline[1], 'r')

#Violation
ax2 = plt.twinx()
ax2.plot([x + 35 for x in violationTimeslot.keys()], violationTimeslot.values(), 'go', markersize=5)
ax2.set_ylim([0, 33])
ax2.grid(True)
ax2.set_xlim(xaxes)
ax2.set_xticks(np.arange(0, xaxes[1], 20))
ax2.set_yticks(np.arange(0, 33, 2))

ax.legend(legend, loc='upper left')
#print(arrivalRateT, arrivalRate)
ax.grid(True)
ax.set_xlim(xaxes)
ax.set_xticks(np.arange(0, xaxes[1], 20))
#axes.set_yscale('log')
# axes.set_yticks([1, 10000, 100000, 500000])
ax.set_ylim([0, 60000])
ax.set_xlabel('Index (s)')
ax.set_ylabel('Rate (messages per second)')
plt.title('Total Arrival Rate')
plt.savefig(output_path + jobname + '_TotalArrivalRateWithV.png')
plt.close(fig)

