# -*- coding: utf-8 -*-
import matplotlib
matplotlib.use('Agg')
import sys
userLatency = 1000
userWindow = 1000
base = 1000 #timeslot size
peakIntervals = [[0, 120], [7200, 7290]]
calculateInterval = [120, 720]  #The interval we calculate violation percentage from 1st tuple completed, remove initial part
#totalLength = 7100
substreamLatencies = {} # Dict { substreamId : [[Arrival, Completed]...]}
from os import listdir
#figureName = 'stock_5_64_5_L4T4a0.5_64'
#figureName = '1h_32_L1T10A0.3333333'
figureName = sys.argv[1]
#figureName = 'B7500C120R5000_APP1587972038474_0034'
inputDir = '/home/samza/GroundTruth/wordcount_result/' + figureName + '/'
outputDir = 'figures/' + figureName + '/'
txtOutputFile = 'wordcountSuccessRate.txt'
keyAverageLatencyFlag = False
keyAverageLatencyThreshold = 0.2
keyLatencyIntervalFlag = False
import sys
startTime = sys.maxint
totalTime = 0
totalViolation = 0
violationInPeak = []
totalInPeak = []
ret = []
#Translate time from second to user window index
for peakI in range(0, len(peakIntervals)):
    violationInPeak += [0]
    totalInPeak += [0]
    peakIntervals[peakI]= [peakIntervals[peakI][0] * base / userWindow, peakIntervals[peakI][1] * base / userWindow]
xaxes = [calculateInterval[0] * 1000 / userWindow, calculateInterval[-1] * 1000 / userWindow]

maxMigrationTime = 0
maxMigrationExecutor = ""
migrationTimes = []
runloopStartPoints = {}
restoreStartPoints = {}
shutdownStartPoints = {}
endPoints = {}
lastGT = {}
locality = {}
for fileName in listdir(inputDir):
    if(fileName <> '000001.txt' and fileName.startswith('000')):
        inputFile = inputDir + fileName
        counter = 0
        print("Processing file " + inputFile)
        runloopStartPoint = []
        restoreStartPoint = []
        shutdownStartPoint = []
        endPoint = []
        startLogicTime = sys.maxint
        startOETime = sys.maxint
        t1 = 0
        with open(inputFile) as f:
            lines = f.readlines()
            for i in range(0, len(lines)):
                line = lines[i]
                split = line.rstrip().split(' ')

                counter += 1
               # if (counter % 5000 == 0):
                    #print("Processed to line:" + str(counter))

                if(split[0] == 'GT:'):
                    ttime = long(split[1])
                    if (ttime != 0 and ttime < startTime):
                        startTime = ttime
                        #print(startTime)
                    pid = int(split[3])
                    if(pid not in substreamLatencies):
                        substreamLatencies[pid] = {}
                    if(ttime not in substreamLatencies[pid]):
                        substreamLatencies[pid][ttime] = [0, 0]
                    substreamLatencies[pid][ttime][0] += int(split[5])
                    substreamLatencies[pid][ttime][1] += long(split[7])
                    lastGT[fileName] = ttime
                if(split[0] == 'container' and split[1] == 'start' and split[2] == 'at'):
                    locality[fileName] = split[3]
                if(split[0] == 'Starting' and split[1] == 'run'):
                    restoreStartPoint += [long(split[3])]
                if(split[0] == 'Entering'):
                    runloopStartPoint += [long(split[3])]
                if(split[0] == 'Shutdown'):
                    endPoint += [long(split[2])]
                if(split[0] == 'Shutting'):
                    shutdownStartPoint += [long(split[3])]
        migrationTime = []
        restoreStartPoints[fileName] = restoreStartPoint
        runloopStartPoints[fileName]  = runloopStartPoint
        shutdownStartPoints[fileName] = shutdownStartPoint
        endPoints[fileName] = endPoint
        for i in range(0, len(endPoint)):
            if(i + 1< len(runloopStartPoint)):
                migrationTime += [runloopStartPoint[i + 1] - endPoint[i]]
                migrationTimes += [migrationTime[-1]/1000.0]
        if(len(migrationTime) > 0):
            mmaxMigrationTime = max(migrationTime)
            if(mmaxMigrationTime > maxMigrationTime):
                maxMigrationTime = mmaxMigrationTime
                maxMigrationExecutor = fileName
#            print(fileName, mmaxMigrationTime)
#            print(startPoint, endPoint)
print(maxMigrationTime, maxMigrationExecutor)

#Calculate total live time and migration time
totalLiveTime = 0
totalRestoreTime = 0
restoreTimeRange = [10000000000L, 0]
totalMigrationTime = 0
numberOfMigration = 0
migrationTimeRange = [10000000000000L, 0]
hostLiveTime = {}
for fileName in restoreStartPoints.keys():
    print(fileName)
    # live time
    if(fileName in restoreStartPoints and len(restoreStartPoints[fileName]) > 0):
        beginTime = restoreStartPoints[fileName][0] - startTime
        if(beginTime < xaxes[0] * 1000):
            beginTime = xaxes[0] * 1000
        if(fileName in lastGT):
            endTime = lastGT[fileName] + 1000 - startTime
        else:
            endTime = beginTime
        if(len(endPoints[fileName]) > 0 and endTime < endPoints[fileName][-1] - startTime):
            endTime = endPoints[fileName][-1] - startTime

        if(endTime > xaxes[1] * 1000):
            endTime = xaxes[1] * 1000
        #print(fileName, beginTime, endTime, lastGT[fileName], startTime)

        # per host live time
        if(fileName in locality):
            host = locality[fileName]
            if(host not in hostLiveTime):
                hostLiveTime[host] = 0
            print(host,beginTime, endTime)
            if(endTime > beginTime):
                hostLiveTime[host] += endTime - beginTime

        if(endTime > beginTime):
            totalLiveTime += endTime - beginTime
        #migrationTime
        migrationTime = 0
        restoreTime = 0


        for i in range(-1, len(endPoints[fileName])):
            # Sync time
            if(i == -1):
                if(i + 1 < len(restoreStartPoints[fileName])):
                    LL = restoreStartPoints[fileName][i + 1]
                else:
                    LL = 1000000000000000000l
            else:
                LL = shutdownStartPoints[fileName][i]
            if(i >= len(runloopStartPoints[fileName]) - 1):
                if(i>=0 and i < len(endPoints[fileName])):
                    RR = endPoints[fileName][i]
                else:
                    RR = 0
            else:
                RR = runloopStartPoints[fileName][i + 1]
            LL -= startTime
            RR -= startTime
            if(LL<= xaxes[0] * 1000):
                LL = xaxes[0] * 1000
            if(RR >= xaxes[1] * 1000):
                RR = xaxes[1] * 1000
            if(not (RR <= xaxes[0] * 1000 or LL >= xaxes[1] * 1000)):
                tt = RR - LL
                migrationTime += tt
                numberOfMigration += 1
                if(tt < migrationTimeRange[0]):
                    migrationTimeRange[0] = tt
                if(tt > migrationTimeRange[1]):
                    migrationTimeRange[1] = tt 
        #print(fileName, migrationTime)
        totalMigrationTime += migrationTime
#exit()
# #Draw migration length histogram
# if(True):
#     print("Draw migration length histogram...")
#     import os
#     outputFile = outputDir + 'migrationTimes.png'
#     if not os.path.exists(outputDir):
#         os.makedirs(outputDir)
#     import numpy as np
#     import matplotlib.pyplot as plt
#
#     legend = ['Migration Times Length']
#     fig = plt.figure(figsize=(45, 30))
#     bins = np.arange(0, 20, 1).tolist() + np.arange(20, 100, 10).tolist()
#     plt.hist(migrationTimes, bins=bins)
#     axes = plt.gca()
#     axes.set_xticks(bins)
#     axes.set_yticks(np.arange(0, 200, 10).tolist())
#     plt.grid(True)
#     plt.xlabel('Migration Length(s)')
#     plt.ylabel('# of Migration')
#     plt.title('Migration Time Length')
#     plt.savefig(outputFile)
#     plt.close(fig)

#print(startTime)
#exit(0)
substreamTime = []
substreamViolation = []

substreamLatency = []

totalViolationSubstream = {}
substreamSuccessRate = []
# Draw average latency
for substream in sorted(substreamLatencies):
    #print("Calculate substream " + substream)

    #print(substreamWindowCompletedAndTotalLatency)
    x = []
    y = []
    thisTime = (xaxes[1] - xaxes[0] + 1)
    for peak in range(0, len(peakIntervals)):
        totalInPeak[peak] += (peakIntervals[peak][1] - peakIntervals[peak][0] + 1)
    #thisTime = 0
    thisViolation = 0
    thisViolationInterval = []
    for timeslot in sorted(substreamLatencies[substream]):
        latency = substreamLatencies[substream][timeslot][1]
        number = substreamLatencies[substream][timeslot][0]
        time = (timeslot - startTime) / base
        #print(time)
        x += [time]
        if(number > 0):
            #thisTime += 1
            avgLatency = float(latency) / number
            y += [avgLatency]
            if(time >= xaxes[0] and time <= xaxes[1]):
                if(avgLatency > userLatency):
                    thisViolation += 1

                    if(substream not in totalViolationSubstream):
                        totalViolationSubstream[substream] = []
                    totalViolationSubstream[substream].append(time)
                    if(len(thisViolationInterval) > 0 and thisViolationInterval[-1][1] == time - 1):
                        thisViolationInterval[-1][1] = time
                    else:
                        thisViolationInterval.append([time, time])
            #Calculate peak interval
            for i in range(0, len(peakIntervals)):
                if(time >= peakIntervals[i][0] and time <= peakIntervals[i][1]):
                    if(avgLatency > userLatency):
                        violationInPeak[i] += 1

    substreamTime += [thisTime]
    substreamViolation += [thisViolation]
    percentage = 0.0
    if(thisTime > 0):
        percentage = thisViolation / float(thisTime)
    #print(str(substream), percentage, thisTime)
    substreamSuccessRate.append(1 - percentage)
    totalTime += thisTime
    totalViolation += thisViolation

    if(keyAverageLatencyFlag):
        print("Draw ", substream, " violation percentage...")
        import os
        outputFile = outputDir + 'windowLatency/' + substream + '.png'
        if not os.path.exists(outputDir + 'windowLatency'):
            os.makedirs(outputDir + 'windowLatency')
        import numpy as np
        import matplotlib.pyplot as plt
        legend = ['Window Average Latency']
        fig = plt.figure(figsize=(45, 30))
        plt.plot(x, y, 'bs')

        # Add user requirement
        userLineX = [xaxes[0], xaxes[1]]
        userLineY = [userLatency, userLatency]
        userLineC = 'r'
        plt.plot(userLineX, userLineY, linewidth=3.0, color=userLineC, linestyle='--')

        plt.legend(legend, loc='upper left')
        # print(arrivalRateT, arrivalRate)
        plt.grid(True)
        axes = plt.gca()
        axes.set_xlim(xaxes)
        axes.set_ylim([1, 10**6])
        axes.set_yscale('log')
        plt.xlabel('Timeslot Index')
        plt.ylabel('Average Latency')
        plt.title('Window Average Latency')
        plt.savefig(outputFile)
        plt.close(fig)
    if(keyLatencyIntervalFlag):
        x = []
        for i in range(0, len(thisViolationInterval)):
            #print(thisViolationInterval[i])
            x += [thisViolationInterval[i][1] - thisViolationInterval[i][0] + 1]
        import os
        outputFile = outputDir + 'latencyInterval/' + substream + '.png'
        if not os.path.exists(outputDir + 'latencyInterval'):
            os.makedirs(outputDir + 'latencyInterval')
        import numpy as np
        import matplotlib.pyplot as plt
        legend = ['Latency Interval']
        fig = plt.figure(figsize=(45, 30))
        plt.hist(x, bins=range(0,200))
        axes = plt.gca()
        axes.set_xticks(range(0,200))
        axes.set_yticks(np.arange(0, 200, 5).tolist())
        plt.grid(True)
        plt.xlabel('Latency Interval Length')
        plt.ylabel('# of Interval')
        plt.title('Latency Interval')
        plt.savefig(outputFile)
        plt.close(fig)

# draw substream violation

import numpy as np
import matplotlib.pyplot as plt
import os

if not os.path.exists(outputDir):
    os.makedirs(outputDir)
outputFile = outputDir + 'violation.png'
legend = ['Total substream violation']
figList = []
for substreamList in totalViolationSubstream:
    #print(substreamList)
    figList.append(len(totalViolationSubstream[substreamList]))
plt.plot(figList, 'r^')
plt.xlabel('Timeslot Index')
plt.ylabel('#substream violation')
plt.title('Total substream violation')
plt.savefig(outputFile)

#Draw substream violation percetage histogram
if(True):
    print("Draw overall violation percentage figure...")
    import os
    outputFile = outputDir + 'keyViolationPercentage.png'
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    import numpy as np
    import matplotlib.pyplot as plt
    legend = ['Violation Percentage']
    fig = plt.figure(figsize=(45, 30))
    x = []
    for i in range(0, len(substreamTime)):
        x += [substreamViolation[i] / float(substreamTime[i])]
    bins = np.arange(0, 0.2, 0.01).tolist() + np.arange(0.2, 1, 0.05).tolist()
    plt.hist(x, bins=bins)
    axes = plt.gca()
    axes.set_xticks(bins)
    axes.set_yticks(np.arange(0, 1000, 50).tolist())
    plt.grid(True)
    plt.xlabel('Violation Percentage')
    plt.ylabel('# of Keys')
    plt.title('Keys Violation Percentage')
    plt.savefig(outputFile)
    plt.close(fig)

avgViolationPercentage = totalViolation / float(totalTime)
sumDeviation = 0.0
print('avg success rate=', 1 - avgViolationPercentage)
ret.append(1 - avgViolationPercentage)
print('total violation number=' + str(totalViolation))
violationNotPeak = totalViolation
timeNotPeak = totalTime
if(totalViolation > 0):
    for peakI in range(0, len(peakIntervals)):
        print('violation percentage in peak '+str(peakI) + ' is ' + str(violationInPeak[peakI]/float(totalViolation)) + ', number is ' + str(violationInPeak[peakI]))
        violationNotPeak -= violationInPeak[peakI]
        timeNotPeak -= totalInPeak[peakI]
print('Execept peak avg success rate=', 1 - violationNotPeak/float(timeNotPeak))
print(substreamSuccessRate)
if(numberOfMigration == 0):
    numberOfMigration = 1
    totalMigrationTime = 0
print(totalLiveTime)
print("Total live time: %.5f , total migration time: %.3f ,  ratio: %.5f , avg: %.5f range: %s\n" % (totalLiveTime/1000.0 ,  totalMigrationTime/1000.0, totalMigrationTime/float(totalLiveTime), totalMigrationTime/float(numberOfMigration), str(migrationTimeRange)))

#per host live time
print(hostLiveTime)

ret += [totalLiveTime/1000.0 , totalMigrationTime/1000.0, totalMigrationTime/float(totalLiveTime), totalMigrationTime/1000.0/float(numberOfMigration), migrationTimeRange[0]/1000.0, migrationTimeRange[1]/1000.0]
ret.append(substreamSuccessRate)
ret.append(hostLiveTime)
# Calculate avg latency
with open(txtOutputFile, 'a') as f:
    f.write("%.15f\t%.3f\t%.3f\t%.15f\t%.3f\t%.3f\t%.3f\n%s\n%s\n\n" % (ret[0], ret[1], ret[2], ret[3], ret[4], ret[5], ret[6], ret[7], ret[8]))

if(False):
    print("Calculate avg lantecy")
    sumLatency = 0
    totalTuples = 0
    for i in range(0, len(substreamLatency)):
        #print(substreamLatency[i])
        sumLatency += sum(substreamLatency[i])
        totalTuples += len(substreamLatency[i])

    avgLatency = sumLatency / float(totalTuples)
    print('avg latency=', avgLatency)

    # Calculate standard error
    sumDeviation = 0.0
    print("Calculate standard deviation")
    for i in range(0, len(substreamLatency)):
        for j in range(0, len(substreamLatency[i])):
            sumDeviation += (((substreamLatency[i][j] - avgLatency) ** 2) / (totalTuples-1)) ** (0.5)
    print('Standard deviation=', sumDeviation)
    print('Standard error=', sumDeviation/(totalTuples) ** (0.5))

