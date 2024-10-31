#include <stdlib.h>
#include <stdio.h>
#include "threadpool.h"



ThreadPool_t *ThreadPool_create(unsigned int num) {
    ThreadPool_t *tp = malloc(sizeof(ThreadPool_t));
    tp->threads = malloc(num * sizeof(pthread_t));

    tp->jobs.size = 0;
    tp->jobs.head = NULL;
        
    /*for (unsigned int i = 0; i < num; i++) {
        pthread_create(&tp->threads[i], NULL, (void *)Thread_run, tp);
    }*/

    return tp;
}


void ThreadPool_destroy(ThreadPool_t *tp) {
    
}


bool ThreadPool_add_job(ThreadPool_t *tp, thread_func_t func, void *arg, unsigned int funcType, unsigned int priority) {
    // Create a new job
    ThreadPool_job_t *job = malloc(sizeof(ThreadPool_job_t)); 
    job->func = func;
    job->arg = arg;
    job->priority = priority;
    job->funcType = funcType;

    // Insert the job in ascending order by priority
    ThreadPool_job_t **current = &tp->jobs.head;
    while (*current != NULL && (*current)->priority < job->priority) {
        current = &(*current)->next;
    }

    job->next = *current;
    *current = job;

    return true;
}


ThreadPool_job_t *ThreadPool_get_job(ThreadPool_t *tp) {
    if (tp->jobs.head == NULL)
        return NULL;
    ThreadPool_job_t *job = tp->jobs.head;
    tp->jobs.head = tp->jobs.head->next;
    return job;
}

void *Thread_run(ThreadPool_t *tp) {
    ThreadPool_job_t *job = ThreadPool_get_job(tp);
    if (job != NULL) {
        job->func(job->arg);
        printf("done processing: %s\n", (char *)job->arg);
    }

    return NULL;
}


void ThreadPool_check(ThreadPool_t *tp) {

}

