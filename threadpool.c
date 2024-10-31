#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>
#include "threadpool.h"



ThreadPool_t *ThreadPool_create(unsigned int num) {
        ThreadPool_t *tp = malloc(sizeof(ThreadPool_t));
        tp->threads = malloc(num * sizeof(pthread_t));

        tp->jobs.size = 0;
        tp->jobs.head = NULL;
        
        for (unsigned int i = 0; i < num; i++) {
                pthread_create(&tp->threads[i], NULL, (void *)Thread_run, tp);
        }

        return tp;
}


void ThreadPool_destroy(ThreadPool_t *tp) {
    
}

/*bool ThreadPool_add_job(ThreadPool_t *tp, thread_func_t func, void *arg) {
    struct stat jobFile_st;

    if (stat((char *)arg, &jobFile_st) != 0) {
        return false;
    }
    
    ThreadPool_job_t *job = malloc(sizeof(ThreadPool_job_t));
    job->func = func;
    job->arg = arg;
    
    ThreadPool_job_t **current = &tp->jobs.head;
    struct stat currentFile_st;
    // Traverse the queue looking for the next biggest file size
    while (current != NULL) {
        stat((char *)(*current)->arg, &currentFile_st);
        if (currentFile_st.st_size > jobFile_st.st_size) {
            break;
        }
        current = &(*current)->next;
    }

    // Insert the job at the previous position making the queue in ascending order of file size
    job->next = *current;
    *current = job;
    return false;
}*/


bool ThreadPool_add_job(ThreadPool_t *tp, thread_func_t func, void *arg) {
    struct stat jobFile_st;

    // Get the new jobs file size
    if (stat((char *)arg, &jobFile_st) != 0) {
        return false;   // File doesnt exist or some other error
    }

    // Create a new job
    ThreadPool_job_t *job = malloc(sizeof(ThreadPool_job_t)); 
    job->func = func;
    job->arg = arg;
    job->next = NULL;

    // Insert the job in ascending order by file size
    ThreadPool_job_t **current = &tp->jobs.head;
    struct stat currentJobFile_st;

    // Traverse the list to find the right insertion point
    while (*current != NULL) {
        stat((char *)(*current)->arg, &currentJobFile_st);
        if (currentJobFile_st.st_size > jobFile_st.st_size) {
            break;
        }
        current = &(*current)->next;
    }

    // Insert the job at the found position
    job->next = *current;
    *current = job;

    return true;
}


ThreadPool_job_t *ThreadPool_get_job(ThreadPool_t *tp) {
    return NULL;
}

void *Thread_run(ThreadPool_t *tp) {
    return NULL;
}

void Mytr(thread_func_t func, void *arg) {
    func(arg);
    printf("done processing\n");
}


void ThreadPool_check(ThreadPool_t *tp) {

}

