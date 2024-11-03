#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mapreduce.h"
#include "threadpool.h"

PartitionTable_t *pt;

void showPartitionTable(unsigned int limited) {
    for (unsigned int i = 0; i < pt->size; i++) {
        PartitionTable_bucket_t bucket = pt->buckets[i];
        printf(" %-5d [%d]->", bucket.size, i);
        PartitionTable_node_t *node = bucket.head;
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
        if (job->funcType == 0) {
            printf("[%s]->", (char *)job->arg);
        } else if (job->funcType == 1) {
            PartitionTable_bucket_t *bucket = (PartitionTable_bucket_t *)job->arg;
            printf("[%s, %d]->", bucket->head->key, bucket->index);
        }
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
   
    pt = malloc(sizeof(PartitionTable_t));
    pt->buckets = malloc(num_parts * sizeof(PartitionTable_bucket_t));
    pt->size = num_parts;
    
    for (unsigned int i = 0; i < num_parts; i++) {
        pt->buckets[i].size = 0;
        pt->buckets[i].index = i;
    }

    ThreadPool_t *tp = ThreadPool_create(num_workers);
    for (unsigned int i = 0; i < file_count; i++) {
        ThreadPool_add_job(tp, (void *)mapper, (void *)(file_names[i]));
    }
     
    showThreadPoolJobs(tp);
    printline();
    Thread_run(tp);
    printline();
    showThreadPoolJobs(tp);

    // Mapper done -> free thread pool
    ThreadPool_destroy(tp);

    printline();
    showPartitionTable(5);
    printline();
    

    // Change this to SJF
    for (unsigned int i = 0; i < pt->size; i++) {
        if (pt->buckets[i].size == 0)
            continue;   // No keyvalue pairs to process for bucket[i]
        Threadarg_t *threadarg = malloc(sizeof(Threadarg_t));
        threadarg->func = (void *)reducer;
        threadarg->partition_idx = i;
        MR_Reduce(threadarg);
    }
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
        reducer(head->key, ta->partition_idx);
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

