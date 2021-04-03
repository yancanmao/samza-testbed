# -*- coding: utf-8 -*-

import sys

jobname = sys.argv[1]
input_file = '/home/samza/GroundTruth/nexmark_result/' + jobname + "/000001.txt"
# input_file = 'GroundTruth/stdout'
output_path = 'figures/' + jobname + '/'
xaxes = [0, 1955]  # [0, 1355]#[0000, 755]
deltaT = 100
latency_requirement = 1
startline = [[155, 155], [0, 10000000]]
executorsFigureFlag = False

colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k']
lineType = ['--', '-.', ':']
markerType = ['+', 'o', 'd', '*', 'x']

substreamArrived = {}
substreamArrivedT = {}
substreamCompleted = {}
substreamCompletedT = {}
containerArrived = {}
containerArrivedT = {}
containerBacklog = {}
containerBacklogT = {}
containerArrivalRate = {}
containerArrivalRateT = {}
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
    if (initialTime == -1):
        initialTime = long(time)
    info = "".join(split[6:]).replace(' ', '')
    info = info.replace('{', '')
    info = info.replace('}', '')
    containers = info.split(',')
    total = 0
    for container in containers:
        if (len(container) > 0):
            Id = container.split('=')[0]
            value = container.split('=')[1]
            if (value == 'NaN'): value = '0'
            total += float(value) * 1000
            if (Id not in containerArrivalRate):
                containerArrivalRate[Id] = []
                containerArrivalRateT[Id] = []
            containerArrivalRate[Id] += [float(value) * 1000]
            containerArrivalRateT[Id] += [(long(time) - initialTime) / base]
            if ((long(time) - initialTime) / base not in totalArrivalRate):
                totalArrivalRate[(long(time) - initialTime) / base] = 0.0
            totalArrivalRate[(long(time) - initialTime) / base] += float(value) * 1000

def parseContainerArrived(split, base):
    global initialTime
    time = split[2]
    if (initialTime == -1):
        initialTime = long(time)
    info = "".join(split[5:]).replace(' ', '')
    info = info.replace('{', '')
    info = info.replace('}', '')
    containers = info.split(',')
    for container in containers:
        if (len(container) > 0):
            Id = container.split('=')[0]
            value = container.split('=')[1]
            if (value == 'NaN'): value = '0'
            if (Id not in containerArrived):
                containerArrived[Id] = []
                containerArrivedT[Id] = []
            containerArrived[Id] += [long(value)]
            containerArrivedT[Id] += [(long(time) - initialTime) / base]

def parseContainerBacklog(split, base):
    global initialTime
    time = split[2]
    if (initialTime == -1):
        initialTime = long(time)
    info = "".join(split[5:]).replace(' ', '')
    info = info.replace('{', '')
    info = info.replace('}', '')
    containers = info.split(',')
    for container in containers:
        if (len(container) > 0):
            Id = container.split('=')[0]
            value = container.split('=')[1]
            if (value == 'NaN'): value = '0'
            if (Id not in containerBacklog):
                containerBacklog[Id] = []
                containerBacklogT[Id] = []
            containerBacklog[Id] += [long(value)]
            containerBacklogT[Id] += [(long(time) - initialTime) / base]

def parsePartitionArrived(split, base):
    global initialTime
    time = split[2]
    if (initialTime == -1):
        initialTime = long(time)
    info = "".join(split[6:]).replace(' ', '')
    info = info.replace('{', '')
    info = info.replace('}', '')
    containers = info.split(',')
    for container in containers:
        if (len(container) > 0):
            Id = container.split('=')[0]
            value = container.split('=')[1]
            if (value == 'NaN'): value = '0'
            if (Id not in substreamArrived):
                substreamArrived[Id] = []
                substreamArrivedT[Id] = []
            substreamArrived[Id] += [long(value)]
            substreamArrivedT[Id] += [(long(time) - initialTime) / base]

def parsePartitionCompleted(split, base):
    global initialTime
    time = split[2]
    if (initialTime == -1):
        initialTime = long(time)
    info = "".join(split[6:]).replace(' ', '')
    info = info.replace('{', '')
    info = info.replace('}', '')
    containers = info.split(',')
    for container in containers:
        if (len(container) > 0):
            Id = container.split('=')[0]
            value = container.split('=')[1]
            if (value == 'NaN'): value = '0'
            if (Id not in substreamCompleted):
                substreamCompleted[Id] = []
                substreamCompletedT[Id] = []
            substreamCompleted[Id] += [long(value)]
            substreamCompletedT[Id] += [(long(time) - initialTime) / base]

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
        if (counter % 100 == 0):
            print("Processed to line:" + str(counter))

        if ((split[0] == 'DelayEstimateModel,' or split[0] == 'Model,') and split[4] == 'Arrival' and split[
            5] == 'Rate:'):
            parseContainerArrivalRate(split, base)
        if ((split[0] == 'DelayEstimateModel,' or split[0] == 'Model,') and split[4] == 'Arrived:'):
            parseContainerArrived(split, base)
        if ((split[0] == 'DelayEstimateModel,' or split[0] == 'Model,') and split[4] == 'Backlog:'):
            parseContainerBacklog(split, base)
        if (split[0] == 'State,' and split[4] == 'Partition' and split[5] == 'Arrived:'):
            parsePartitionArrived(split, base)
        if (split[0] == 'State,' and split[4] == 'Partition' and split[5] == 'Completed:'):
            parsePartitionCompleted(split, base)

        # Add migration marker
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
            if (split[1] == 'Scale' and split[2] == 'in'):
                decision += [-1]
            elif (split[1] == 'Scale' and split[2] == 'out'):
                decision += [1]
            else:
                decision += [0]

        if (split[0] == 'Number' and split[2] == 'severe'):
            time = int(lines[i - 1].split(' ')[2])
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
    if (Id not in migrationDecisionTime):
        return lines
    for i in range(len(migrationDecisionTime[Id])):
        if (migrationDecisionTime[Id][i] > 0):  # Migrate out
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
    if (Id not in migrationDeployTime):
        return lines
    for i in range(len(migrationDeployTime[Id])):
        if (migrationDeployTime[Id][i] > 0):  # Migrate out
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

partialNumber = 10
# Draw substream arrived & completed. Find backlog
for substream in substreamArrived.keys():
    substreamArrivedT[substream] = [i * deltaT / 1000.0 for i in substreamArrivedT[substream]]
    substreamCompletedT[substream] = [i * deltaT / 1000.0 for i in substreamCompletedT[substream]]
    print("Drawing " + substream + " arrived...")
    legend = ['Arrived', 'Completed']
    fig = plt.figure(figsize=(60, 30))
    partialIndex = []
    #partialIndex = [x for x in range(0, len(substreamCompleted[substream]))
    #        if substreamCompleted[substream][x] < 1000000 / partialNumber]
    #print(partialIndex)
    #plt.plot([substreamArrivedT[substream][x] for x in partialIndex], [substreamArrived[substream][x] for x in partialIndex], 'b')
    #plt.plot([substreamCompletedT[substream][x] for x in partialIndex],
    #         [substreamCompleted[substream][x] for x in partialIndex], 'g')

    plt.plot(substreamArrivedT[substream], substreamArrived[substream], 'b')
    plt.plot(substreamCompletedT[substream], substreamCompleted[substream], 'g')

    violatedPoint = []
    j = 0
    for i in range(0, len(substreamCompleted[substream])):
        while j < len(substreamArrived[substream]) and substreamArrived[substream][j] < substreamCompleted[substream][i]:
            j += 1
        if i > 0 and substreamCompleted[substream][i] > substreamCompleted[substream][i - 1] and substreamCompletedT[substream][i] - substreamArrivedT[substream][j] >= latency_requirement:
            violatedPoint.append(i)

    #indexN = latency_requirement * 1000 / deltaT
    #violatedPoint = [x for x in range(0, len(substreamCompleted[substream]))
    #    if x >= indexN and substreamCompleted[substream][x] <= substreamArrived[substream][x - indexN]]

    print("Violations " + str(violatedPoint))
    for point in violatedPoint:
        x0 = substreamCompletedT[substream][point]
        x1 = substreamCompletedT[substream][point] - latency_requirement
        y0 = substreamCompleted[substream][point]
        y1 = substreamCompleted[substream][point]
        plt.plot([x0, x1], [y0, y1], 'r')
    plt.plot(startline[0], startline[1], 'r')
    plt.legend(legend, loc='upper left')
    axes = plt.gca()
    axes.set_xticks(np.arange(0, xaxes[1], 20))  # 755, 20))
    #axes.set_xlim([0, substreamCompletedT[substream][partialIndex[-1]]])
    axes.set_xlim(xaxes)
    axes.set_ylim([0, 1000000])
    print("Drawing...")
    plt.xlabel('Index (s)')
    plt.ylabel('# of Tuples')
    plt.title('Substream ' + substream + ' Arrived & Completed')
    import os
    if not os.path.exists(output_path + 'substreams/'):
        os.makedirs(output_path + 'substreams/')
    plt.savefig(output_path + 'substreams/Arrived_' + substream + '.png')
    plt.close(fig)
