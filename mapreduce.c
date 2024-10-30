#include <stdio.h>
#include <stdlib.h>
#include "mapreduce.h"
#include "threadpool.h"

PartitionTable_t PartitionTable;

void showPartitionTable() {
    for (unsigned int i = 0; i < PartitionTable.size; i++) {
        printf("%d", i);

        /*PartitionTableBucket_t bucket = PartitionTable.buckets[i];
        PartitionTableNode_t *node = bucket.head;
        printf("[%d]", i);
        if (node == NULL)
            continue;
        printf("->[key: %s, value %s]", node->key, node->value); 
        while (node->next != NULL) {
            node = node->next;
            printf("->[key: %s, value %s]", node->key, node->value);
        }*/
        printf("\n");
    }
}


/**
* Run the MapReduce framework
* Parameters:
*     file_count   - Number of files (i.e. input splits)
*     file_names   - Array of filenames
*     mapper       - Function pointer to the map function
*     reducer      - Function pointer to the reduce function
*     num_workers  - Number of threads in the thread pool
*     num_parts    - Number of partitions to be created
*/
void MR_Run(unsigned int file_count, char *file_names[],
            Mapper mapper, Reducer reducer, 
            unsigned int num_workers, unsigned int num_parts) {
	// Create the partition table for the key value pair
    PartitionTable.buckets = malloc(num_parts * sizeof(PartitionTable_t));
    // Start each bucket with size 0
    for (unsigned int i = 0; i < num_parts; i++) {
        PartitionTable.buckets[i].size = 0;
    }
    PartitionTable.size = num_parts;

    // Create the worker threads
	ThreadPool_t *threadPool = ThreadPool_create(num_workers);

    // Add the jobs to the job queue
	for (unsigned int i = 0; i < file_count; i++) {
       ThreadPool_add_job(threadPool, (void *)mapper, file_names[i]); 
    }
   
    /**
     * Queue test. output the filenames stored in the queue from the args for the map function
    ThreadPool_job_t *currentJob = threadPool->jobs.head;
    for (unsigned int i = 0; i < file_count; i++) {
        printf("job arg %s\n", (char *)currentJob->arg);
        currentJob = currentJob->next;
    }
    */
    
    Thread_run(threadPool);
    printf("testingpoint\n");
    showPartitionTable();
}

/**
* Write a specifc map output, a <key, value> pair, to a partition
* Parameters:
*     key           - Key of the output
*     value         - Value of the output
*/
void MR_Emit(char *key, char *value) {
    // Get the hash of the key
    unsigned int hash = MR_Partitioner(key, PartitionTable.size);
    PartitionTableBucket_t currentBucket = PartitionTable.buckets[hash];
    // Add the key value pair to the partition table
    PartitionTableNode_t *node = malloc(sizeof(PartitionTableNode_t));
    node->key = key;
    node->value = value;
    if (currentBucket.head != NULL) {
        // Set the current head as the next of the new node
        node->next = currentBucket.head;
   } 
   // Add the new node as the head and increment the size
   currentBucket.head = node;
   currentBucket.size += 1;
}

/**
* Hash a mapper's output to determine the partition that will hold it
* Parameters:
*     key           - Key of a specifc map output
*     num_partitions- Total number of partitions
* Return:
*     unsigned int  - Index of the partition
*/
unsigned int MR_Partitioner(char *key, unsigned int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

/**
* Run the reducer callback function for each <key, (list of values)> 
* retrieved from a partition
* Parameters:
*     threadarg     - Pointer to a hidden args object
*/
void MR_Reduce(void *threadarg) {

}

/**
* Get the next value of the given key in the partition
* Parameters:
*     key           - Key of the values being reduced
*     partition_idx - Index of the partition containing this key
* Return:
*     char *        - Value of the next <key, value> pair if its key is the current key
*     NULL          - Otherwise
*/
char *MR_GetNext(char *key, unsigned int partition_idx) {
	return NULL;
}

