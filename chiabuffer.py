#!/usr/bin/env python3
import signal
import sys
import shutil
import glob
from datetime import datetime
import time

ext = "plot"
minSize = 200

#be sure to include the trailing / on the directory
sources = ['/home/nopes/buffer/']
destinations = ['/home/nopes/farm/sea35/']

jobs = []

stopSignal=False

def exit_gracefully(signum, frame):
    global stopSignal
    # restore the original signal handler as otherwise evil things will happen
    # in raw_input when CTRL+C is pressed, and our signal handler is not re-entrant
    signal.signal(signal.SIGINT, original_sigint)

    stopSignal=True

    # restore the exit gracefully handler here    
    signal.signal(signal.SIGINT, exit_gracefully)


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

    #scan sources, create file list 
    for source in sources:
        for file in glob.glob(source + "*." + ext):
            if stopSignal:
                pass

            file_count += 1

            try:

               # next lets scan the name, rename to plot.2.tmp, copy, and rename again 
               shutil.move(file, jobs[file_count % len(jobs)])
               print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + ' ' + file + ':  copied successfully')

            # If source and destination are same
            except shutil.SameFileError:
                print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + ' ' + file + ': Source and destination '
                                                                                   'represents the same file in '
                      + (jobs[file_count % len(jobs)]))

            # If there is any permission issue
            except PermissionError:
                print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + ' ' + file + ": Permission denied.")

            # For other errors
            except:
                print("Error occurred while copying file.")


if __name__ == "__main__":
    
    original_sigint = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, exit_gracefully)    
    
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")

    print('Started Buffer Process: ' + current_time)
    print('Press Ctrl+C To exit, File currently in process will complete before exit.')
   
    #loop so i dont have to do this manually 
    while True:
        # dont slam the disk if we have no files
        time.sleep(10)
        if not stopSignal: 
            main()
        else:
            sys.exit(0);







