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

#be sure to include the trailing / on the directory
sources = [f'/home/{user}/buffer/']
destinations = [
                 f'/home/{user}/farm/sea36/'
                ,f'/home/{user}/farm/sea37/'
                ,f'/home/{user}/farm/sea38/'
                ,f'/home/{user}/farm/sea39/'
                 ]

jobs = []



#globals 
stopSignal=False
executor = None
in_progress = list()
once=True
file_count = -1
throttle = 0
ctrl_c_press_num = 0

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
        self.in_progress = []   #list of source in copy
        self.usable = number_of_workers
        self.q = queue.Queue(number_of_workers)
        self.active = True
        for i in range(number_of_workers):
            threading.Thread(target=self.worker, daemon=True, args=(i,) ).start()

    def setUsable( self, num ):
        self.usable = num

    def addJob(self, source, dest):
        #post job to queue
        self.q.put((source, dest))  

    def size(self):
        return self.q.qsize()

    def empty(self):
        return self.q.empty()

    def full(self):
        return self.q.full()

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

            #print(f'Thread {number} . started')
            
            #pull from queue, run next job
            source, dest = self.q.get()
            self.in_progress.append(source)
            print( datetime.now().strftime("%m/%d/%Y, %H:%M:%S")  + f' -- [{number}] Started {source} -> {dest} ')
            
            try:
                copy_one_plot(source, dest, number)
            except Exception as e: 
                print(e)

            self.in_progress.remove(source)
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

    # do not restore the exit gracefully handler here. two cntrl-c will take us out
    #signal.signal(signal.SIGINT, exit_gracefully)



def main( pool ):
    global once
    global stopSignal
    global file_count
    global in_progress
    global throttle

    diskUsage = {}

    if once:
        once=False
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

#    if throttle % 20 == 0:
#        print('Available Destinations: %s ' % jobs)
#        print('Destination : %s %s /%s /%s ' %  )

    throttle = throttle + 1
    #print('Jobs running %s' % pool.size())

    #scan sources, create file list 
    for source in sources:
        for filename in glob.glob(source + "*." + ext):
            if stopSignal:
                pass

            file_count += 1
            #print(f'starting {filename}')
            try:
                destfilename = jobs[file_count % len(jobs)]

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

    #fix this, awful hack to do this twice
    for dest in destinations:
        total, used, free = shutil.disk_usage(dest)
        #only add det if it has enough space. 
        if (free // (2 ** 30)) > minSize :
            #print( f'appending dest = {dest}')
            job_predict.append(dest)

    size = len(job_predict)
    
    pool = JobPool( size )
    print( size )

    print('Started Buffer Process: ' + current_time,'  Num parallel: '+ str(size))
    print('Press Ctrl+C To exit, File currently in process will complete before exit.')

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







