#include <stdio.h>
#include <stdlib.h>
#include "mapreduce.h"
#include "threadpool.h"

PartitionTable_t PartitionTable;
int testint = 0;

void showPartitionTable() {
    printf("\nshowPartitionTable:\n");
    for (unsigned int i = 0; i < PartitionTable.size; i++) {
        printf("[%d] --> ", i);
        
        PartitionTableBucket_t *bucket = &PartitionTable.buckets[i];
        PartitionTableNode_t *node = bucket->head;
        while (node != NULL) {
            printf("[%s, %s] --> ", node->key, node->value);
            node = node->next;
        }
        
        printf("NULL\n");
    }
}

void showJobQueue(ThreadPool_t *tp) {
    printf("showJobQueue:   ");
    ThreadPool_job_t *job = tp->jobs.head;
    while (job != NULL) {
        printf("[%s] --> ", (char *)job->arg);  // Print job argument
        job = job->next;  // Move to the next job
    }
    
    printf("NULL\n");
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
    PartitionTable.buckets = malloc(num_parts * sizeof(PartitionTableBucket_t));
    // Start each bucket with size 0
    for (unsigned int i = 0; i < num_parts; i++) {
        PartitionTable.buckets[i].size = 0;
    }
    PartitionTable.size = num_parts;

    // Create the worker threads
    //ThreadPool_t *threadPool = ThreadPool_create(num_workers);

    // Add the jobs to the job queue
	//for (unsigned int i = 0; i < file_count; i++) {
      // ThreadPool_add_job(threadPool, (void *)mapper, (void *)file_names[0]); 
    //}

    //showJobQueue(threadPool);
   
        
    //Thread_run(threadPool);
    printf("\n\ntestint: %d\n\n", testint);

    Mytr((void *)mapper, (void *)file_names[0]);
    printf("testingpoint\n");
    showPartitionTable();

    printf("\n\ntestint: %d\n\n", testint);
}

/**
* Write a specifc map output, a <key, value> pair, to a partition
* Parameters:
*     key           - Key of the output
*     value         - Value of the output
*/
void MR_Emit(char *key, char *value) {
    testint += 1;
    // Get the hash of the key
    unsigned int hash = MR_Partitioner(key, PartitionTable.size);
    PartitionTableBucket_t *bucket = &PartitionTable.buckets[hash];
    // Add the key value pair to the partition table
    PartitionTableNode_t *node = malloc(sizeof(PartitionTableNode_t));
    node->key = key;
    node->value = value;

    // Insert the new node at the head of the linked list in the partition table
    node->next = bucket->head; 
    bucket->head = node;

    bucket->size += 1;
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

