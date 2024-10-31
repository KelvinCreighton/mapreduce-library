#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include "mapreduce.h"
#include "threadpool.h"

int MR_EmitTest_int = 0;

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
    
    pt = malloc(sizeof(PartitionTable_t));
    pt->buckets = malloc(num_parts * sizeof(PartitionTable_bucket_t));
    pt->size = num_parts;

    for (unsigned int i = 0; i < num_parts; i++) {
        pt->buckets[i].size = 0;
    }

    ThreadPool_t *tp = ThreadPool_create(num_workers);
    for (unsigned int i = 0; i < file_count; i++) {
        struct stat st;
        if (stat(file_names[i], &st)) {
            continue; // File doesnt exist
        }
        ThreadPool_add_job(tp, (void *)mapper, (void *)(file_names[i]), st.st_size);
    }

    showThreadPoolJobs(tp);
    printline();
    Thread_run(tp);
    printline();
    showThreadPoolJobs(tp);
    
    printline();
    showPartitionTable(5);
    printline();

    printf("MR_Emit() run count: %d\n", MR_EmitTest_int);
}


void MR_Emit(char *key, char *value) {
    MR_EmitTest_int += 1;
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

}

char *MR_GetNext(char *key, unsigned int partition_idx) {
        return NULL;
}

