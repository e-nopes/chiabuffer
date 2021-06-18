#!/usr/bin/env python3
import signal
import sys
import shutil
import glob
from datetime import datetime
import time
import threading
import multiprocessing as mp


ext = "txt"
minSize = 200

#be sure to include the trailing / on the directory
sources = ['/home/nopes/buffer/']
destinations = ['/home/nopes/farm/sea35/']

jobs = []

stopSignal=False

'''
class Dest( ) :
    __init__ ( self, dest ):
        self.location = dest
        self.spaceRemain = 0

class Host:
    __init__ ( self, loc ):
        self.location = loc
        #self.spaceFull 

class Job ( self, source, dest ) :
    __init__:
        self.name = ''
        host = Host( source, dest);
    def run:
        shutil.move(filename,destfilename )
        print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + ' ' + file + ':  copied successfully')
'''

class WaitTimeIndicator:
    def __init__(self):
        self.init_time = time.clock_gettime(time.CLOCK_MONOTONIC)

    def start(self):
        print( '\n Waited %d seconds' % (time.clock_gettime(time.CLOCK_MONOTONIC) - self.init_time) )
    
    def tick(self):
        print( '\r Waited %d seconds' % (time.clock_gettime(time.CLOCK_MONOTONIC) - self.init_time) )

    def stop(self):
        self.init_time = time.clock_gettime(CLOCK_MONOTONIC)


def double_move_file( source, dest ) :
    # next lets scan the name, rename to plot.2.tmp, copy, and rename again 
    #dest_filename =  dest )]
    tmp_ext = '.chia2.tmp' 
     
    shutil.move(source, source+tmp_ext )
    shutil.copy(source+tmp_ext, dest+tmp_ext)
    shutil.move(dest+tmp_ext, dest)


def copy_one_plot ( source, dest ) :
    # next lets scan the name, rename to plot.2.tmp, copy, and rename again 
    tmp_ext = '.chia.tmp' 
    
    try:     
        shutil.move(source, source+tmp_ext )
        shutil.copy(source+tmp_ext, dest+tmp_ext)
        shutil.move(dest+tmp_ext, dest)

    except Exception as e: 
        print(e)


class JobPool:
    def __init__ (self, number_of_threads):
        self.threads = []
        self.max_pool_size = number_of_threads
        #allocate threads
        mp.set_start_method('spawn')
        self.q = mp.Queue()
        #make our threads iterable
        threads.insert(self.q)

    def start(self, filename, destfilename):
        print(' spawning ')
        self.p = mp.Process(target=copy_one_plot, args=( filename, destfilename ))
        self.q.start()

    def stop(self):
        for jobs in self.threads:
            print(' joining job  ') 
            jobs.join()
        
        





def exit_gracefully(signum, frame):
    global stopSignal
    # restore the original signal handler as otherwise evil things will happen
    # in raw_input when CTRL+C is pressed, and our signal handler is not re-entrant
    signal.signal(signal.SIGINT, original_sigint)

    stopSignal=True

    print(' please wait ')

    # restore the exit gracefully handler here    
    signal.signal(signal.SIGINT, exit_gracefully)

#thread id identifies join
threadpool = dict()

#keep file count between scans
file_count = -1

def main():
    global stopSignal
    global file_count

    #signal.signal(signal.SIGINT, signal_handler)
    jobs.clear()
    
    #scan our destinations
    for dest in destinations:
        total, used, free = shutil.disk_usage(dest)
        #only add det if it has enough space
        if (free // (2 ** 30)) > minSize:
                jobs.append(dest)
    
    print('dests : %s ' % jobs)
    #pool = Thread.Threa()
    
    #scan sources, create file list 
    for source in sources:
        for filename in glob.glob(source + "*." + ext):
            if stopSignal:
                pass

            file_count += 1
            print('starting')
            try:
                destfilename = jobs[file_count % len(jobs)]
                print('spawning move %s : %s ' % (filename , destfilename) )

                x = threading.Thread(target=copy_one_plot, args=( filename, destfilename ) )
                x.start()
                
#                shutil.move(filename, destfilename )
                print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + ' ' + filename + ':  started successfully')

##             # next lets scan the name, rename to plot.2.tmp, copy, and rename again 
               #shutil.move(filename, ])
               #print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + ' ' + filename + ':  copied successfully')

                print (' waiting..')
                x.join()
                print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + ' ' + filename + ':  copied successfully')
                print (' joined..')

            # If source and destination are same
            except shutil.SameFileError:
                print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + ' ' + filename + ': Source and destination '
                                                                                   'represents the same file in '
                                                                                    + (jobs[file_count % len(jobs)]))

            # If there is any permission issue
            except PermissionError:
                print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + ' ' + filename + ": Permission denied.")

            # For other errors
            except (shutil.Error ):
                print("Shutil OS Error occurred while copying filename.")
            except Exception as e: 
                print(e)


if __name__ == "__main__":

    print(f"Arguments count: {len(sys.argv)}")
    for i, arg in enumerate(sys.argv):
        print(f"Argument {i:>6}: {arg}")
    
    if sys.argv[1]:
        print('        \n', sys.argv[1])
        time_in_secs = int( sys.argv[1] )

    original_sigint = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, exit_gracefully)    
    
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")

    print('Started Buffer Process: ' + current_time)
    print('Press Ctrl+C To exit, File currently in process will complete before exit.')
    display_time = WaitTimeIndicator()
    display_time.start()
    #loop so i dont have to do this manually 
    while True:
        time.sleep(time_in_secs)
        # dont slam the disk if we have no files
        display_time.start()
        display_time.tick()
        if not stopSignal: 
            main()
        else:
            sys.exit(0);







