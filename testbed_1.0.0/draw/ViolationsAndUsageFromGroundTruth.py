# -*- coding: utf-8 -*-
def draw(n, userLatency, userWindow, l, h, figureName):
    #userLatency = 5000
    #userWindow = 5000
    print(n, userLatency, userWindow, l, h, figureName)
    retValue = [n, userLatency, userWindow, l, h]
    base = 1000 #timeslot size
    peakIntervals = [[0, 90], [7200, 7290]]
    calculateInterval = [0000, 14400]  #The interval we calculate violation percentage from 1st tuple completed
    #totalLength = 7100
    substreamArrivalAndCompletedTime = {} # Dict { substreamId : [[Arrival, Completed]...]}
    substreamNumber = 64 # -1 for using stock id
    from os import listdir
    #figureName = 'stock_5_64_5_L4T4a0.5_64'
    #figureName = '1h_32_L1T10A0.3333333'
    #figureName = '4h_16_L5T5l120'
    inputDir = '/home/samza/GroundTruth/' + figureName + '/'
    outputDir = 'figures/' + figureName + '/'
    unionBidFlag = True
    unionBidNumber = [9954, 7955, 4566, 4367, 18672, 10543, 5670, 6634, 16108, 4922, 7018, 5216, 5823, 5733, 8090, 5030, 10036, 4077, 7158, 10781, 4856, 6127, 4295, 5155, 5013, 5700, 6602, 6195, 10296, 6031, 6913, 6853, 3658, 6453, 7579, 7246, 9113, 4935, 7329, 5368, 7272, 6021, 6146, 8608, 5313, 4662, 7955, 10148, 5597, 3846, 7188, 4703, 5206, 4449, 9389, 3809, 5084, 2960, 6869, 5165, 4953, 3995, 3409, 9692]
    #unionBidNumber = [x + 1 for x in unionBidNumber]
    unionBidStartTime = 0
    migrationIntervalFlag = True
    keyAverageLatencyFlag = False
    keyAverageLatencyThreshold = 0.25
    keyLatencyIntervalFlag = False
    calibrateFlag = False
    import sys
    startTime = sys.maxint
    totalTuples = 0
    tuplesIncludeUnion = 0
    totalTime = 0
    totalViolation = 0
    violationInPeak = []
    totalInPeak = []

    # Translate time from second to user window index
    for peakI in range(0, len(peakIntervals)):
        violationInPeak += [0]
        totalInPeak += [0]
        peakIntervals[peakI]= [peakIntervals[peakI][0] * base / userWindow, peakIntervals[peakI][1] * base / userWindow]
    xaxes = [calculateInterval[0] * 1000 / userWindow, calculateInterval[-1] * 1000 / userWindow]
    
    maxMigrationTime = 0
    maxMigrationExecutor = ""
    migrationTimes = []
    if(unionBidFlag):
        partitionRe = {}
        for fileName in sorted(listdir(inputDir)):
            if (fileName <> '000001.txt' and fileName.startswith('000')):
                inputFile = inputDir + fileName
                print("Processing file " + inputFile)
                with open(inputFile) as f:
                    lines = f.readlines()
                    for i in range(0, len(lines)):
                        line = lines[i]
                        split = line.rstrip().split(' ')
                        if (split[0] == 'stock_id:'):
                            if(int(split[1]) not in partitionRe):
                                partitionRe[int(split[1])] = []
                            partitionRe[int(split[1])].append(int(split[5]))
                            tuplesIncludeUnion += 1
        for partition in partitionRe.keys():
            t = sorted(partitionRe[partition])[unionBidNumber[partition]]
            print('partition ' + str(partition) + ' time ' + str(t))
            if(t > unionBidStartTime):
                unionBidStartTime = t
        print("Union bid over at:" + str(unionBidStartTime))
        print("Tuples: " + str(tuplesIncludeUnion))
        partitionRe.clear()
    for fileName in listdir(inputDir):
        if(fileName <> '000001.txt' and fileName.startswith('000')):
            inputFile = inputDir + fileName
            counter = 0
            print("Processing file " + inputFile)
            startPoint = []
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
                    if(calibrateFlag and split[0] == 'start' and split[1] == 'time:'):
                        t = int(split[2])
                        if(startOETime > t):
                            startOETime = t

                    if(split[0] == 'stock_id:'):
                        if(int(split[5]) > unionBidStartTime):
                            if (not calibrateFlag and int(split[3]) < startTime):
                                startTime = int(split[3])
                            if(substreamNumber == -1):
                                keyid = split[1]
                            else:
                                keyid = int(split[1]) % substreamNumber
                            if(keyid not in substreamArrivalAndCompletedTime):
                                substreamArrivalAndCompletedTime[keyid] = []
                            if (calibrateFlag):
                                if(startLogicTime > int(split[3])):
                                    startLogicTime = int(split[3])
                                if(startTime > startLogicTime):
                                    startTime = startLogicTime
                                completeTime = int(split[5]) - startOETime + startLogicTime

                                arrivalTime = int(split[3])
                                #skip 11:30 ~ 1:00
                                if(int(split[3]) > 1284377500000):
                                    arrivalTime -= 5400000
                                if(startLogicTime > 1284377500000):
                                    completeTime -= 5400000
                                #print(arrivalTime, completeTime)
                                substreamArrivalAndCompletedTime[keyid].append([str(arrivalTime), str(completeTime)])
                            else:
                                substreamArrivalAndCompletedTime[keyid].append([split[3], split[5]])
                                totalTuples += 1
                    if(split[0] == 'Entering'):
                        startPoint += [int(split[3])]
                    if(split[0] == 'Shutdown'):
                        endPoint += [int(split[2])]
            migrationTime = []
            for i in range(0, len(endPoint)):
                if(i + 1< len(startPoint)):
                    migrationTime += [startPoint[i + 1] - endPoint[i]]
                    migrationTimes += [migrationTime[-1]/1000.0]
            if(len(migrationTime) > 0):
                mmaxMigrationTime = max(migrationTime)
                if(mmaxMigrationTime > maxMigrationTime):
                    maxMigrationTime = mmaxMigrationTime
                    maxMigrationExecutor = fileName
                print(fileName, mmaxMigrationTime)
                print(startPoint, endPoint)

    print('total tuples=' + str(totalTuples))

    print(maxMigrationTime, maxMigrationExecutor)

    #Draw migration length histogram
    if(migrationIntervalFlag):
        print("Draw migration length histogram...")
        import os
        outputFile = outputDir + 'migrationTimes.png'
        if not os.path.exists(outputDir):
            os.makedirs(outputDir)
        import numpy as np
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        

        legend = ['Migration Times Length']
        fig = plt.figure(figsize=(45, 30))
        bins = np.arange(0, 20, 1).tolist() + np.arange(20, 100, 10).tolist()
        plt.hist(migrationTimes, bins=bins)
        axes = plt.gca()
        axes.set_xticks(bins)
        axes.set_yticks(np.arange(0, 200, 10).tolist())
        plt.grid(True)
        plt.xlabel('Migration Length(s)')
        plt.ylabel('# of Migration')
        plt.title('Migration Time Length')
        plt.savefig(outputFile)
        plt.close(fig)

    #print(startTime)
    #exit(0)
    substreamTime = []
    substreamViolation = []
    substreamSuccessRate = []
    substreamLatency = []
    # Draw average latency
    for substream in sorted(substreamArrivalAndCompletedTime):
        print("Calculate substream " + str(substream))
        substreamWindowCompletedAndTotalLatency = {}
        latencys = []
        for pair in substreamArrivalAndCompletedTime[substream]:
            arriveTime = int(pair[0])
            completeTime = int(pair[1])
            latency = completeTime - arriveTime
            if(latency < 0):
                #print("What? " + str(substream) + " " + str(pair))
                latency = 0

            if((completeTime - startTime)/userWindow >= xaxes[0] and (completeTime - startTime)/userWindow <= xaxes[1]):
                latencys += [latency]

            timeslot = (completeTime - startTime)/userWindow
            if(timeslot not in substreamWindowCompletedAndTotalLatency):
                substreamWindowCompletedAndTotalLatency[timeslot] = [1, latency]
            else:
                substreamWindowCompletedAndTotalLatency[timeslot][0] += 1
                substreamWindowCompletedAndTotalLatency[timeslot][1] += latency
        #print(substreamWindowCompletedAndTotalLatency)
        substreamLatency.append(latencys)
        x = []
        y = []
        thisTime = (xaxes[1] - xaxes[0] + 1)
        for peak in range(0, len(peakIntervals)):
            totalInPeak[peak] += (peakIntervals[peak][1] - peakIntervals[peak][0] + 1)
        #thisTime = 0
        thisViolation = 0
        thisViolationInterval = []
        for time in sorted(substreamWindowCompletedAndTotalLatency):
            latency = substreamWindowCompletedAndTotalLatency[time][1]
            number = substreamWindowCompletedAndTotalLatency[time][0]
            #print(time)
            x += [time]
            if(number > 0):
                #thisTime += 1
                avgLatency = float(latency) / number
                y += [avgLatency]
                if(time >= xaxes[0] and time <= xaxes[1]):
                    if(avgLatency > userLatency):
                        thisViolation += 1
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
        substreamSuccessRate += [1.0 - percentage]
        print(str(substream), percentage, thisTime)
        totalTime += thisTime
        totalViolation += thisViolation

        if(keyAverageLatencyFlag and percentage > keyAverageLatencyThreshold):
            print("Draw ", substream, " violation percentage...")
            import os
            outputFile = outputDir + 'windowLatency/' + str(substream) + '.png'
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
            outputFile = outputDir + 'latencyInterval/' + str(substream) + '.png'
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

    #Draw substream violation percetage histogram
    if(False):
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
    retValue += [1 - avgViolationPercentage]
    print('total violation number=' + str(totalViolation))
    print(substreamSuccessRate)
    retValue.append(substreamSuccessRate)
    violationNotPeak = totalViolation
    timeNotPeak = totalTime
    if(totalViolation > 0):
        for peakI in range(0, len(peakIntervals)):
            print('violation percentage in peak '+str(peakI) + ' is ' + str(violationInPeak[peakI]/float(totalViolation)) + ', number is ' + str(violationInPeak[peakI]))
            violationNotPeak -= violationInPeak[peakI]
            timeNotPeak -= totalInPeak[peakI]
    print('Violation in peak=', 1 - violationNotPeak/float(totalViolation),' avg success rate=', 1 - violationNotPeak/float(timeNotPeak))
    retValue.append(1 - violationNotPeak/float(totalViolation))
    retValue.append(1 - violationNotPeak/float(timeNotPeak))
    return retValue
    # Calculate avg latency
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

inputFile = 'inputdir.txt'
outputFile = 'successrate.txt'

with open(inputFile) as f:
    lines = f.readlines()
    for i in range(0, len(lines)):
        line = lines[i]
        split = line.rstrip().split(' ')
        if(len(split) == 1):
            fileName = split[0]
            index = fileName.index('_') + 1
            n = int(fileName[index: fileName[index:].index('_') + index])
            index = fileName.index('L') + 1
            print(n, index)
            L = int(fileName[index: fileName.index('T')]) * 1000
            index = fileName.index('T', index) + 1
            T = int(fileName[index: fileName.index('l')]) * 1000
            index = fileName.index('l') + 1
            l = int(fileName[index: index + fileName[index:].index('h')]) / 1000.0
            index = index + fileName[index:].index('h') + 1
            if('d' in fileName):
                h = int(fileName[index: fileName.index('d')]) / 1000.0
                deltaT = int(fileName[fileName.index('d') + 1: fileName.index('d') + 1 + fileName[fileName.index('d') + 1:].index('m')])
                index = fileName.index('d') + 1 + fileName[fileName.index('d') + 1:].index('m')
                if('_' in fileName[index + 1:]):
                    m = int(fileName[index + 1: index + 1 + fileName[index + 1:].index('_')])
                else:
                    m = int(fileName[index + 1:])
            else:
                h = int(fileName[index: index + fileName[index:].index('_')]) / 1000.0
                deltaT = 100
        else: #static
            L = int(split[0]) * 1000
            T = int(split[1]) * 1000
            l = 0.0
            h = 0.0
            fileName = split[2]
            index = fileName[7:].index('_')
            n = int(fileName[7: 7 + index])
            deltaT = 100
            m = 0
        print('n, L ,T, l, h, deltaT, m=', n, L, T, l, h, deltaT, m)
        ret = draw(n, L, T, l, h, fileName)
        #ret = [0, 0, 0, 0.1, 0.9, [], 0.9, 0.9]
        #print(ret)
        if('static' not in fileName):
            from RateAndWindowDelay import draw as ratedraw
            ret1 = ratedraw(deltaT, fileName)
        else:
            index = fileName[7:].index('_')
            ret1 = [int(fileName[7: 7 + index]), 0, 0, 0]
        print(ret)
        with open(outputFile, 'a') as f:
            f.write("%s\n%d\t%d\t%d\t%.3f\t%.3f\t%.15f\t%s\t%d\t%d\t%d\t%.15f\t%.15f\n%s\n" % (fileName, ret[0], ret[1]/1000, ret[2]/1000, ret[3], ret[4], ret[5], ret1[0], ret1[1], ret1[2], ret1[3], ret[7], ret[8], ret[6]))
