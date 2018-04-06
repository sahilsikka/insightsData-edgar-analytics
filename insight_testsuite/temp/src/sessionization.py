import csv
from datetime import datetime
from datetime import timedelta
from collections import OrderedDict

ipToTime = OrderedDict()
timeToIP = OrderedDict()

def sessionalize():
    text_file = open("./output/sessionization.txt", "w")
    #reading from input file
    with open('./input/log.csv', 'r') as inputFile:
            f = open('./input/inactivity_period.txt','r')
            inactivityTime = int(f.read())

            fileStream = csv.reader(inputFile)
            next(fileStream)
            for row in fileStream:
                key = row[0]  #ip
                date = row[1]  #date-time
                time = row[2]

                sessionEntry = datetime.strptime(date + "," + time, "%Y-%m-%d,%H:%M:%S")

                #check for ending session time#
                sessionTimeLast = sessionEntry-timedelta(seconds=inactivityTime)

                ipSessionOut = []
                #taking the first ip whose session is to be checked
                for j in list(timeToIP):
                    if j<sessionTimeLast:
                        ipSessionOut.append(timeToIP[j])
                        timeToIP.pop(j)
                        break;

                #saving ips whose session is over
                if(len(ipSessionOut)!=0):
                    for ip in ipSessionOut[0]:
                        latestIp=ipToTime[ip]
                        latestTimeOfIpSession = latestIp[len(ipToTime[ip])-1]
                        if(latestTimeOfIpSession<sessionTimeLast):
                            ipSessionInformation=formatOutput(ip,latestIp)
                            text_file.write(ipSessionInformation)
                            ipToTime.pop(ip)

                ipToTime.setdefault(key, []).append(sessionEntry)
                timeToIP.setdefault(sessionEntry, []).append(key)

            #saving ips after the file has reached endpoint
            for ip in list(ipToTime):
                sessionTimes=ipToTime[ip]
                ipSessionInformation= formatOutput(ip,sessionTimes)
                text_file.write(ipSessionInformation)
                ipToTime.pop(ip)

            timeToIP.clear()
    inputFile.close()

def formatOutput(ip,sessionTimes):
    length = len(sessionTimes)
    sessionDuration = sessionTimes[length - 1] - sessionTimes[0] + timedelta(seconds=1)
    filesRequested = length
    initialTime = sessionTimes[0].strftime('%Y-%m-%d %H:%M:%S')
    finalTime = sessionTimes[len(sessionTimes) - 1].strftime('%Y-%m-%d %H:%M:%S')
    ipInfor = ip + ',' + initialTime + ',' + finalTime + ',' + str(sessionDuration.total_seconds()).split('.')[
        0] + ',' + str(filesRequested)+'\n'
    return ipInfor

sessionalize()