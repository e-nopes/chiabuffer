# chiabuffer

Will spawn as many threads as you have destination disks (or a maximum), copies files to each destination in parallel

Python script to monitor a Buffer drive and move plots to an array of dest drives. Will not include destination drives with less than `minSize` amount.

Usage:

    python3 chiabuffer.py

Settings to edit in .py file:
    ext = "plot" # extension we are looking for (you can test usuing .txt files, etc)
    minSize = 800 # Size in GB - if a dest drive has less free space remaining, it's removed from list
    sources = ["/mnt/from/"] # Comma separated list of dirs to monitor (/ at end is needed for linux!)
    destinations = ["/mnt/to/"] # comma separated list of dirs to move plots into (/ at end is needed for linux!)
    max_concurrent = 4  # max number of parallel copies to spawn, regardless of destination



Notes:
    When chia creates plots, it does so as .plot.2.tmp. As a final action the files are 
    renamed to .plot. 
    
    This tool will rename any existing and new .plot files to .plot.x.tmp, move them to one of the destinations, 
    and rename them .plot

    We scan the directory in sources for 'ext' files , and move files to a group of destinations.
    the files are more or less randomly thrown into any drive with available space.
    the intent is to parallelize the distribution, while only doing 1 copy at a time per destination.

    It does no cleanup. hit ctrl-c 3 times at your own risk.
    
    If you have to ctrl-c out. rename your temp/*.plot.x.tmp files to .plot .
    Also you should delete the partial plot.x.tmp file on your destination drive.
