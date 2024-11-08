# - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Name : Kelvin Creighton
# SID : 1695352
# CCID : kcreight
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - -


Synchronization (using pthread):

    - Variables created:
        - One mutex variable was used in the ThreadPool_t struct and one was used in my defined PartitionTable_t struct. These were used to lock and unclock a critical section when the possiblility of multiple threads could interfere with each other
        - Additionally one conditional number (cond) variable was used in the ThreadPool_t struct to allow threads to "pause" and "unpause" when a specific condition was met or not met.

    - ThreadPool_t Sync (mutex/cond) (this was used in the following functions):
        - ThreadPool_add_job(): before calling this function to allow only one thread to create the job queue of either the Mapper or Reducer stages
        - Thread_run(): when calling ThreadPool_get_job() to get a job in the job queue
            - additionally an "active_workers" variable was used to keep track of the total active workers/threads currently executing a job. it would increment per thread prior to starting the job and decrement per thread after finishing executing the job.
            This "active_workers" variable was used in combination with the Thread_run() and ThreadPool_check() functions to "pause" threads until all threads have finished executing after the job queue was emptied. This would ensure that the threads would not proceed to the next stage of the program while work was being finished.
            The "active_workers" thread was also used for creating the job queue and was set to a 1 when creating and then to 0 when finished to ensure that the other threads would wait for the one thread to finish creating the job queue, similar to how they would wait for the last thread to finish its work before continueuing to the next stage of the program
        - ThreadPool_check(): the mutex lock and conditional variable (cond) was used here to pause the threads while the active_workers count was greater than 0. it would signal a cont_wait to avoid busy waiting then when the last worker finished it would singal a cond_signal to unpause the threads. This would ensure that no extra CPU usage was processed with busy waiting while also synchronizing the threads to wait for finished work.

    fil- PartitionTable_t Sync (mutex):
        - the mutex in the PartitionTable_t struct was used to allow only one thread at a tie to access the partition of the partition table. if multiple threads were to access them at the same time then they could cause issues for both getting or both setting or each getting and setting. thus a mutex was used here to force only one thread to get or set a node in the partition of the partition table.

    - In general pthread_mutex_lock and pthread_mutex_unlock were used to lock and unlock the mutex of each struct. pthread_cond_wait and pthread_cond_signal were used to pause and unpause the threads instead of busy waiting. Also the pthread_mutex/cond_init and pthread_mutex/cond_destroy were used to create and destory each mutex and conditional number and avoid memory leaks



Partitions and the PartitionTable:
    - The partitions for the keyvalue pairs are stored as a PartitionTable (or hashtable) where each is hashed using the total number of paritions and then indexed into the partition as a linked list. These partitions are sorted by ASCII to allow easy searching of the keyvalue pairs during the Reduce phase of the program. The PartitionTable exists as a global variable for both the Mapper and Reducer phases of the program to be able to access it


Testing:
    - Job queue:
        - The job queue was tested using my own defined function to iterate through the linked list and printf the args of the function for the Mapper stage and the threadargs for the reducer stage. This helped me verify that it was sorted in SJF based on filesize for the mapper stage and partition size for the reducer stage

    - PartitionTable:
        - the partition table was tested in a much similar way to the Job queue while would output each partition with its hash index and show me all the keyvalue pairs associated with the partition in the linked list by iterating through the entire thing. This helped me verify that each partition was sorted by ASCII

    - Custom files:
        - I created (and since deleted for the final git push (but can be found in the older pushes before the last)) a few test files to check for SJF for the job queue allowing me to know the sizes of the files in advance and also the data inside to check for ascending ASCII order for the PartitionTable. 
    
    - Primarily the provided test cases were used with the "./wordcount sample_inputs/sample*.txt" command to load them all and the "cat result-*.txt" to output the result of all the files at once. 



Sources:
    - I used the man pages provided for the pthread library and stat library for getting the file sizes
    - Additionally I used stack overflow for verification on how to use the conditional number appropriately since I found the man pages hard to read and understand at times with minimal examples. (no code was copied just referenced for my own understanding of the specifics of pthread_cond_signal and pthread_cond_wait) 

