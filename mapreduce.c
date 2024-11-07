#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include "mapreduce.h"
#include "threadpool.h"

PartitionTable_t *pt;

void MR_Run(unsigned int file_count, char *file_names[],
            Mapper mapper, Reducer reducer, 
            unsigned int num_workers, unsigned int num_parts) {
    
    // Create the partition table
    pt = malloc(sizeof(PartitionTable_t));
    pt->buckets = malloc(num_parts * sizeof(PartitionTable_bucket_t));
    pt->size = num_parts;
    pthread_mutex_init(&pt->mutex, NULL);
    
    // Allocate each partition as a bucket in the partition table
    for (unsigned int i = 0; i < num_parts; i++) {
        pt->buckets[i].size = 0;
        pt->buckets[i].index = i;
        pt->buckets[i].head = NULL;
    }
    
    // Create the threads
    ThreadPool_t *tp = ThreadPool_create(num_workers);


    tp->active_workers = 1;     // Flag that jobs are being added to the job queue
    tp->func_type = 0;          // Flag to indicate we are at the mapper part of the code
    struct stat st;
    // Add all the files to the job queue with its priority based on the file size
    pthread_mutex_lock(&tp->mutex);
    for (unsigned int i = 0; i < file_count; i++) {
        if (stat(file_names[i], &st) != 0)
            continue;           // Error reading the file
        tp->priority = (unsigned int)st.st_size;
        ThreadPool_add_job(tp, (void *)mapper, (void *)(file_names[i]));
    }
    tp->active_workers = 0;     // Flag that all the jobs have finished being added
    pthread_cond_signal(&tp->cond); // Release the waiting threads
    pthread_mutex_unlock(&tp->mutex);
    
    // Check if the main thread is still adding jobs to the job queue
    ThreadPool_check(tp);

    Thread_run(tp);
    
    ThreadPool_check(tp);

    tp->active_workers = 1;     // Flag that jobs are being added to the job queue
    tp->func_type = 1;  // Flag to indicate that we are at the reducer part of the code
    pthread_mutex_lock(&tp->mutex);
    for (unsigned int i = 0; i < pt->size; i++) {
        if (pt->buckets[i].size == 0)
            continue;   // No keyvalue pairs to process for bucket[i]
        tp->priority = pt->buckets[i].size;
        Threadarg_t *threadarg = malloc(sizeof(Threadarg_t));
        threadarg->func = (void *)reducer;
        threadarg->partition_idx = i;
       ThreadPool_add_job(tp, (thread_func_t)MR_Reduce, (void *)threadarg);
    }
    tp->active_workers = 0;     // Flag that jobs are done being added to the job queue
    pthread_cond_signal(&tp->cond); // Release the waiting threads
    pthread_mutex_unlock(&tp->mutex);

    ThreadPool_check(tp);

    Thread_run(tp);
    
    ThreadPool_check(tp);
    ThreadPool_destroy(tp);
    pthread_mutex_destroy(&pt->mutex);
    free(pt->buckets);
    free(pt);
}


void MR_Emit(char *key, char *value) {
    unsigned int hash = MR_Partitioner(key, pt->size);

    // Create the new node
    PartitionTable_node_t *newNode = malloc(sizeof(PartitionTable_node_t));
    newNode->key = strdup(key);
    newNode->value = strdup(value);
    
    // Iterate through the partition comparing they keys to order them in ascending ASCII order
    pthread_mutex_lock(&pt->mutex);
    PartitionTable_node_t *currentNode = pt->buckets[hash].head;
    // If the partition is empty or if the new key is before the head then place it as the new head
    if (currentNode == NULL || strcmp(key, currentNode->key) <= 0) {
        pt->buckets[hash].head = newNode;
        newNode->next = currentNode;
        pt->buckets[hash].size += 1;
        pthread_mutex_unlock(&pt->mutex);
        return;
    }
    // Iterate through the list finding the place for the new key to keep the partition in ascending ASCII order
    while (currentNode->next != NULL && strcmp(key, currentNode->next->key) > 0) {
        currentNode = currentNode->next;
    }
    PartitionTable_node_t *tmp = currentNode->next;
    currentNode->next = newNode;
    newNode->next = tmp;
    pt->buckets[hash].size += 1;
    pthread_mutex_unlock(&pt->mutex);
}


unsigned int MR_Partitioner(char *key, unsigned int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}


void MR_Reduce(void *threadarg) {
    Threadarg_t *ta = (Threadarg_t *)threadarg;
    Reducer reducer = (Reducer)ta->func;
    pthread_mutex_lock(&pt->mutex);
    PartitionTable_node_t *head = pt->buckets[ta->partition_idx].head;
    while (head != NULL) {
        // Call the Reducer function with the key and partition_idx arguments
        char *key = strdup(head->key);
        pthread_mutex_unlock(&pt->mutex);
        reducer(key, ta->partition_idx);
        free(key);
        pthread_mutex_lock(&pt->mutex);
        head = pt->buckets[ta->partition_idx].head;
        pthread_mutex_unlock(&pt->mutex);
    }
    free(ta);
}

char *MR_GetNext(char *key, unsigned int partition_idx) {
    pthread_mutex_lock(&pt->mutex);
    // Return null if the bucket is empty
    if (pt->buckets[partition_idx].head == NULL) {
        pthread_mutex_unlock(&pt->mutex);
        return NULL;
    }
    
    // If the head key compares with the given key, return the head and point the head to the next node
    PartitionTable_node_t *node = pt->buckets[partition_idx].head;
    if (strcmp(key, node->key) == 0) {
        pt->buckets[partition_idx].head = node->next;
        char *value = strdup(node->value);
        free(node->key);
        free(node->value);
        free(node);
        pthread_mutex_unlock(&pt->mutex);
        return value;
    }

    // Itterate through the rest of the bucket searching for the next node key that compares with the given key
    while (node->next != NULL) {
        if (strcmp(key, node->next->key) == 0) {
            PartitionTable_node_t *temp = node->next;
            char *value = strdup(temp->value);
            node->next = node->next->next;
            free(temp->key);
            free(temp->value);
            free(temp);
            pthread_mutex_unlock(&pt->mutex);
            return value;
        }
        node = node->next;
    }
    pthread_mutex_unlock(&pt->mutex);
    return NULL;
} 

