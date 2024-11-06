#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include "mapreduce.h"
#include "threadpool.h"

PartitionTable_t *pt;

void showPartitionTable(unsigned int limited) {
    for (unsigned int i = 0; i < pt->size; i++) {
        printf(" %-5d [%d]->", pt->buckets[i].size, i);
        PartitionTable_node_t *node = pt->buckets[i].head;
        unsigned int count = 0;
        while (node != NULL) {
           printf("[%s, %s]->", node->key, node->value);
           node = node->next;
           count++;
           if (limited != -1 && count > limited)
               break;
        }
        if (limited != -1 && count > limited)
            printf("...\n");
        else
            printf("[NULL]\n");
    }
}

void showThreadPoolJobs(ThreadPool_t *tp) {
    ThreadPool_job_t *job = tp->jobs.head;
    while (job != NULL) {
            printf("[%s]->", (char *)job->arg);
        job = job->next;
    }
    printf("[NULL]\n");
}

void printline() {
    printf("-------------------------------------------------------\n");
}


void MR_Run(unsigned int file_count, char *file_names[],
            Mapper mapper, Reducer reducer, 
            unsigned int num_workers, unsigned int num_parts) {
    printf("\n\n\n");
    printline();
    
    // Create the partition table
    pt = malloc(sizeof(PartitionTable_t));
    pt->buckets = malloc(num_parts * sizeof(PartitionTable_bucket_t));
    pt->size = num_parts;
    
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

    
    showThreadPoolJobs(tp);
    printline();
    
    Thread_run(tp);
    
    showThreadPoolJobs(tp);
    printline();
    
    showPartitionTable(5);
    printline();
    
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

    showThreadPoolJobs(tp);
    printline();
    
    Thread_run(tp);
    
    showThreadPoolJobs(tp);
    printline();
    
    ThreadPool_check(tp);
    ThreadPool_destroy(tp);
    free(pt->buckets);
    free(pt);
}


void MR_Emit(char *key, char *value) {
    unsigned int hash = MR_Partitioner(key, pt->size);

    // Create new node
    PartitionTable_node_t *node = malloc(sizeof(PartitionTable_node_t));
    node->key = strdup(key);
    node->value = strdup(value);
    
    // Put the node as the head of the bucket in the partition hash table
    node->next = pt->buckets[hash].head;
    pt->buckets[hash].head = node;
    pt->buckets[hash].size += 1;

    // Compare and swap buckets to keep them in ascending order of number of keys
    /*if (hash == pt->size-1)
        return;
    if (pt->buckets[hash].size > pt->buckets[hash+1].size) {
        PartitionTable_bucket_t tmp = pt->buckets[hash];
        pt->buckets[hash] = pt->buckets[hash+1];
        pt->buckets[hash+1] = tmp;
    }*/
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
    PartitionTable_node_t *head = pt->buckets[ta->partition_idx].head;
    Reducer reducer = (Reducer)ta->func;
    while (head != NULL) {
        // Call the Reducer function with the key and partition_idx arguments
        char *key = strdup(head->key);
        reducer(key, ta->partition_idx);
        free(key);
        head = pt->buckets[ta->partition_idx].head;
    }
    free(ta);
}

char *MR_GetNext(char *key, unsigned int partition_idx) {
    if (pt->buckets[partition_idx].head == NULL)
        return NULL;

    PartitionTable_node_t *node = pt->buckets[partition_idx].head;
    if (strcmp(key, node->key) == 0) {
        pt->buckets[partition_idx].head = node->next;
        char *value = strdup(node->value);
        free(node->key);
        free(node->value);
        free(node);
        return value;
    }

 
    while (node->next != NULL) {
        if (strcmp(key, node->next->key) == 0) {
            PartitionTable_node_t *temp = node->next;
            char *value = strdup(temp->value);
            node->next = node->next->next;
            free(temp->key);
            free(temp->value);
            free(temp);
            return value;
        }
        node = node->next;
    }
    return NULL;
} 

