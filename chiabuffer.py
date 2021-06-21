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
minSize = 200
#find home directory
user = getpass.getuser()

#only ever tested with 1 source drive.
sources = [f'/home/{user}/buffer/']

#be sure to include the trailing / on the directory
destinations = [
                 f'/home/{user}/farm/sea36/'
                ,f'/home/{user}/farm/sea37/'
                ,f'/home/{user}/farm/sea38/'
                ,f'/home/{user}/farm/sea39/'
                 ]

jobs = []



#globals 
stopSignal=False
file_count = -1
throttle = 0
ctrl_c_press_num = 0

#verbosity
v = True

def copy_one_plot ( source, dest, num=99 ) :
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
        if v: print (f'[{num}] mv {dest} -> {dest} done')        

    except Exception as e: 
        print(e)



class JobPool():
    def __init__(self, number_of_workers):
        self.usable = number_of_workers
        self.size = number_of_workers
        self.q = queue.Queue(number_of_workers)
        self.active = True
        for i in range(number_of_workers):
            threading.Thread(target=self.worker, daemon=True, args=(i,) ).start()

    def setUsable( self, num ):
        self.usable = num

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
                copy_one_plot(source, dest, number)
            except Exception as e: 
                print(e)

            print( datetime.now().strftime("%m/%d/%Y, %H:%M:%S")  + f' -- [{number}] Finished {source} -> {dest} ')
            self.q.task_done()
            
def exit_gracefully(signum, frame):
    global stopSignal
    global ctrl_c_press_num
    # restore the original signal handler as otherwise evil things will happen
    # in raw_input when CTRL+C is pressed, and our signal handler is not re-entrant
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

    diskUsage = {}
    jobs.clear()

    #no more jobs to be added
    if stopSignal:
        print('Time to Stop')
        return 

    if throttle % 40 == 0:
        print(f' -------------------------  ' )
    #fix this, awful hack to do this twice
    for dest in destinations:
        total, used, free = shutil.disk_usage(dest)
        #only add det if it has enough space. 
        if (free // (2 ** 30)) > minSize :

            total = total // (2**30)
            used =  used  // (2**30)
            free =  free  // (2**30)

            if throttle % 40 == 0:
                print(f' Destination : {dest}  Free Space: {free} GB ' )
            jobs.append(dest)
            #diskUsage.append({dest: (total,used,free)})

    pool.setUsable(len(jobs))

    throttle = throttle + 1
    #scan sources, create file list 
    for source in sources:
        for filename in glob.glob(source + "*." + ext):
            if stopSignal:
                pass

            file_count += 1
            #print(f'starting {filename}')
            try:
                destfilename = jobs[file_count % len(jobs)]
                
                #no doubles
                if os.path.dirname not in jobs:
                    pool.addJob( filename, destfilename)                
                else:
                    print('skipped over %s', filename)

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

    job_predict = []

    #fix this, make rough guess on the nubmer of threads we really want
    for dest in destinations:
        total, used, free = shutil.disk_usage(dest)
        #only add det if it has enough space. 
        if (free // (2 ** 30)) > minSize :
            #print( f'appending dest = {dest}')
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
                time.sleep(1)
        else:
            print(' Waiting for Thread pool cleanup')
            pool.stop()
            sys.exit(0)







