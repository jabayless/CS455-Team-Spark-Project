import time, sys, os, subprocess, concurrent.futures
from threading import Thread

import matplotlib.pyplot as plt
import matplotlib.cbook as cbook

import numpy as np
import pandas as pd

#script to get the runtime and memory uage of java program
#   first argument to program is the path to the program
#   second argument is the java program name
#   example : clear & python3 benchMarks.py /home/jbayless/GitHub/Java/benchMarkScripts/javaTest/build/classes/java/main Test

class BenchMark:
    def __init__(self, classPath, programName):
        self.classPath = classPath
        self.programName = programName
        self.startTime = time.time()
        self.memoryUsage = ""
        self.pid = 0
        self.process = ""

    def getPid(self):
        #get the memory usage of the program
        self.process = subprocess.Popen(["java" , "-cp", self.classPath, self.programName], shell=False, stdout=subprocess.PIPE)
        self.pid = self.process.pid
        return self.pid

    def getMemoryUsage(self):
        memoryCommand = "sudo pmap " + str(self.getPid()) + " | tail -n 1"
        time.sleep(1)
        javaProgram = subprocess.Popen(memoryCommand, shell=True, stdout=subprocess.PIPE)
        self.memoryUsage = str(javaProgram.stdout.read()).split(" ")[11][:-3]
        return self.memoryUsage

    def getRunTime(self):
        return (time.time() - self.startTime)

    def plotData(self):
        headers = ['RANK','CITY','STATE', 'STATION_ID', 'HAPPINESS_SCORE']
        df = pd.read_csv('/home/jbayless/GitHub/Java/benchMarkScripts/happiness_index.csv',names=headers)
        x = df['HAPPINESS_SCORE']
        y = df['RANK']
        plt.xlabel("Happiness Score")
        plt.ylabel("Rank")
        plt.title("Rank vs Happiness Score")

        plt.plot(x,y)
        plt.show()

def main():
        benchMark = BenchMark(sys.argv[1], sys.argv[2])
        print ("Total memory usage is " + str(benchMark.getMemoryUsage()) + ".")
        benchMark.process.wait()
        print ("Time to complete is " + str(time.time() - benchMark.startTime) + "s.")
        #benchMark.plotData()

if __name__ == '__main__':
    main()
