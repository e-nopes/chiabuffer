#!/usr/bin/env python3
import signal
import getpass
import sys
import shutil
import glob
from datetime import datetime
import time
import threading, queue
import os

from concurrent.futures import ThreadPoolExecutor   

ext = "txt"
minSize = 200
#find home directory
user = getpass.getuser()

#be sure to include the trailing / on the directory
sources = [f'/home/{user}/buffer/']
destinations = [f'/home/{user}/farm/sea35/'
                ,f'/home/{user}/farm/sea36/' ]

jobs = []


stopSignal=False
executor = None

#verbosity
v = False

def copy_one_plot ( source, dest, num=99 ) :
    # next lets scan the name, rename to plot.2.tmp, copy, and rename again 
    tmp_ext = '.x.tmp' 
    
    try:     
        filename = os.path.basename(source)
        destfilename = dest+filename
        if v: print (f'[{num}] mv {source} -> {source+tmp_ext} ')
        shutil.move(source, source+tmp_ext )
        if v: print (f'[{num}] mv {source+tmp_ext} -> {destfilename+tmp_ext} ')
        shutil.move(source+tmp_ext, destfilename+tmp_ext)
        if v: print (f'[{num}] mv {destfilename+tmp_ext} -> {destfilename} ')        
        shutil.move(destfilename+tmp_ext, destfilename)
        if v: print (f'[{num}] mv {dest} -> {dest} done')        

    except Exception as e: 
        print(e)


class JobPool():
    def __init__(self, number_of_workers):
        self.in_progress = []   #list of dest in copy
        self.q = queue.Queue(number_of_workers)
        self.active = True
        for i in range(number_of_workers):
            threading.Thread(target=self.worker, daemon=True, args=(i,) ).start()

    def addJob(self, source, dest):
        #one dest folder per job
        print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + ' ' + source + ':  started successfully')   
        #post job to queue
        self.q.put((source, dest))  

    def getCount():
        return self.q.getCount()
    def getSize():
        return self.q.getSize()

    def stop(self):
        if self.active:
            self.active=False
            print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + '  Waiting..')               
            return self.q.join()
    
    def worker(self, number ):
        while self.active:
            #print(f'Thread {number} . started')
            if self.active:
                #pull from queue, run next job
                source, dest = self.q.get()
                self.in_progress.append(source)
                print(f'[{number}] Started {source} -> {dest} ')
                #time.sleep(5)
                copy_one_plot(source, dest,number)
                self.in_progress.remove(source)
                print(f'[{number}] Finished {source} ->  {dest}')
                self.q.task_done()
                

class WaitTimeIndicator:
    def __init__(self):
        self.init_time = time.clock_gettime(time.CLOCK_MONOTONIC)

    def start(self):
        self.init_time = time.clock_gettime(time.CLOCK_MONOTONIC)
    
    def tick(self):
        print( '\r Waited %d seconds' % (time.clock_gettime(time.CLOCK_MONOTONIC) - self.init_time) )


        

def exit_gracefully(signum, frame):
    global stopSignal
    # restore the original signal handler as otherwise evil things will happen
    # in raw_input when CTRL+C is pressed, and our signal handler is not re-entrant
    signal.signal(signal.SIGINT, original_sigint)

    stopSignal=True

    print(' please wait ')

    # restore the exit gracefully handler here    
    signal.signal(signal.SIGINT, exit_gracefully)
from os import listdir
from os.path import isfile, join
in_progress = list()
once=True
#keep file count between scans
file_count = -1

def main( pool ):
    global once
    global stopSignal
    global file_count
    global in_progress

    if once:
        once=False
    jobs.clear()

    #no more jobs to be added
    if stopSignal:
        print('Time to Stop')
        return 
    
    #scan our destinations
    for dest in destinations:
        total, used, free = shutil.disk_usage(dest)
        #only add det if it has enough space. 
        #and only one job per destination at a time
        if (free // (2 ** 30)) > minSize and dest not in jobs:
                jobs.append(dest)
    
    print('Available Destinations: %s ' % jobs)
    
    

    #scan sources, create file list 
    for source in sources:
        for filename in glob.glob(source + "*." + ext):
            if stopSignal:
                pass

            file_count += 1
            print(f'starting {filename}')
            try:
                destfilename = jobs[file_count % len(jobs)]
                print(destfilename, in_progress)
                #next time try to scan for currents?
                if filename not in in_progress:
                    print('spawning move %s : %s ' % (filename , destfilename) )
                    pool.addJob( filename, destfilename)                

            # For other errors
            except (shutil.Error ):
                print("Shutil OS Error occurred while copying filename.")
            except Exception as e: 
                print(e)


if __name__ == "__main__":

    original_sigint = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, exit_gracefully)    
    
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")

    pool = JobPool( len(destinations) )

    print('Started Buffer Process: ' + current_time,'   '+ str(len(destinations)))
    print('Press Ctrl+C To exit, File currently in process will complete before exit.')
    display_time = WaitTimeIndicator()
    display_time.start()
    #loop so i dont have to do this manually 
    while True:
        time.sleep(5)
        # dont slam the disk if we have no files
        display_time.start()

        if not stopSignal: 
            main(pool)
        else:
            print(' Waiting for Thread pool cleanup')
            pool.stop()
            sys.exit(0);







