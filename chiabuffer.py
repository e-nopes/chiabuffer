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
from os import listdir
from os.path import isfile, join

ext = "plot"
minSize = 120

#override destination count with a constant
max_concurrent = 3

# hit ctrl-c to stop the process. 3 times to forcibly break the moves.

#find home directory
user = getpass.getuser()

#only tested with 1 source drive.
sources = [f'/home/{user}/buffer/']

#be sure to include the trailing / on the directory
destinations = [
                 f'/home/{user}/farm/sea34/'
                ,f'/home/{user}/farm/sea21/'
                ,f'/home/{user}/farm/sea10/'
                ,f'/home/{user}/farm/sea36/'
                ,f'/home/{user}/farm/sea37/'
                ,f'/home/{user}/farm/sea38/'
                ,f'/home/{user}/farm/sea39/'
                ,f'/home/{user}/farm/sea40/'
                ,f'/home/{user}/farm/sea41/'
                ,f'/home/{user}/farm/sea43/'
                ]

jobs = []

#globals 
stopSignal=False
file_count = -1
throttle = 0
ctrl_c_press_num = 0

#verbosity
v = False

def move_one_plot ( source, dest, num=99 ) :
    # next lets scan the name, rename to plot.x.tmp, move, and rename again 
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
        if v: print (f'[{num}] mv {source} -> {dest} done')        

    except Exception as e: 
        print(e)


 #manage our queue and 
# pool of workers who feed from the queue
class JobPool():
    def __init__(self, number_of_workers):
        self.usable = number_of_workers
        self.size = number_of_workers
        self.q = queue.Queue(number_of_workers)
        self.active = True
        for i in range(number_of_workers):
            threading.Thread(target=self.worker, daemon=True, args=(i,) ).start()

    # set how many of our workers we should use
    def setUsable( self, num ):
        global max_concurrent
        if num < max_concurrent:
            self.usable = num
        else: 
            self.usable = max_concurrent

    def addJob(self, source, dest):
        #post job to queue
        self.q.put((source, dest))  

    def getUsable(self):
        return self.usable

    def size(self):
        return self.size

    def stop(self):
        if self.active:
            self.active=False
            print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + '  Waiting..')               
            self.q.join()
            print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + '  Joined..')               
    
    def worker(self, number ):
        while self.active:
            #deactivate when requested 
            if (number >= self.usable):
                time.sleep(300) #we are expecting a long sleep
                continue

            #pull from queue, run next job
            source, dest = self.q.get()
            print( datetime.now().strftime("%m/%d/%Y, %H:%M:%S")  + f' -- [{number}] Started {source} -> {dest} ')
            
            try:
                move_one_plot(source, dest, number)
            except Exception as e: 
                print(e)

            print( datetime.now().strftime("%m/%d/%Y, %H:%M:%S")  + f' -- [{number}] Finished {source} -> {dest} ')
            self.q.task_done()
            
def exit_gracefully(signum, frame):
    global stopSignal
    global ctrl_c_press_num

    signal.signal(signal.SIGINT, original_sigint)
    ctrl_c_press_num=ctrl_c_press_num+1
    stopSignal=True

    print(' please wait for threads to finish')

    if ctrl_c_press_num < 3 :
        remain = 3 - ctrl_c_press_num
        print(f' To Exit hit ctrl-c {remain} times. ')
        #keep our handler
        signal.signal(signal.SIGINT, exit_gracefully)

def main( pool ):
    global stopSignal
    global file_count
    global throttle

    #print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + '  Main..Start')               


    diskUsage = {}
    jobs.clear()

    #no more jobs to be added
    if stopSignal:
        print('Time to Stop')
        return 


    if throttle % 8 == 0:
        print(f' -------------------------  ' )
    #fix this, awful hack to do this twice
    for dest in destinations:
        total, used, free = shutil.disk_usage(dest)
        #only add det if it has enough space. 
        if (free // (2 ** 30)) > minSize :

            total = total // (2**30)
            used =  used  // (2**30)
            free =  free  // (2**30)

            if throttle % 8 == 0:
                print(f' Destination : {dest}  Free Space: {free} GB ' )
            jobs.append(dest)

    pool.setUsable(len(jobs))
    throttle = throttle + 1
    
    #scan sources, create file list 
    for source in sources:
        for filename in glob.glob(source + "*." + ext):
            if stopSignal:
                pass

            file_count += 1
            try:
                destfilename = jobs[file_count % len(jobs)]
                
                #no doubles
                pool.addJob( filename, destfilename)                
            
                #If we spawn too fast, we may add multiples before the job changes the filename
                time.sleep(1)

            # For other errors
            except (shutil.Error ):
                print("Shutil OS Error occurred while copying filename.")
            except Exception as e: 
                print(e)

    #print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + '  Main..End')               




if __name__ == "__main__":
    #take signal handler
    original_sigint = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, exit_gracefully)    
    
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    job_predict = []

    #fix this, make rough guess on the nubmer of threads we really want
    for dest in destinations:
        total, used, free = shutil.disk_usage(dest)
        if (free // (2 ** 30)) > minSize :
            job_predict.append(dest)

    size = len(job_predict)
    pool = JobPool( size )

    print('Started Buffer Process: ' + current_time,' Max Num parallel: '+ str(size))
    print('Press Ctrl+C 3x to exit, File currently in process will complete before exit.')

    #sys.exit(0)
    while True:
        if not stopSignal: 
            main(pool)
            
            #30 second update, with 5 second ctrl-c catching
            for i in range(6):
                if stopSignal:
                    continue
                time.sleep(5)
        else:
            print(' Waiting for Thread pool cleanup')
            pool.stop()
            sys.exit(0)



