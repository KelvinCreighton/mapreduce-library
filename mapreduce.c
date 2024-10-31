#include <stdio.h>
#include <stdlib.h>
#include "mapreduce.h"
#include "threadpool.h"

int MR_EmitTest_int = 0;

void showThreadPoolJobs(ThreadPool_t *tp) {
    ThreadPool_job_t *job = tp->jobs.head;
    while (job != NULL) {
        printf("[%s] --> ", (char *)job->arg);
        job = job->next;
    }
    printf("[NULL]\n");
}


void MR_Run(unsigned int file_count, char *file_names[],
            Mapper mapper, Reducer reducer, 
            unsigned int num_workers, unsigned int num_parts) {

    ThreadPool_t *tp = ThreadPool_create(num_workers);
    for (unsigned int i = 0; i < file_count; i++) {
        ThreadPool_add_job(tp, (void *)mapper, (void *)(file_names[i]));
    }

    Mytr((void *)mapper, (void *)file_names[0]);

    printf("\n");
    showThreadPoolJobs(tp);
    printf("\n");

    printf("MR_Emit() run count: %d\n", MR_EmitTest_int);
}


void MR_Emit(char *key, char *value) {
    MR_EmitTest_int += 1;
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

