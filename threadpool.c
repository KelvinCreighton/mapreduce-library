#include <sys/stat.h>
#include <stdlib.h>
#include <stdio.h>
#include "threadpool.h"



ThreadPool_t *ThreadPool_create(unsigned int num) {
    ThreadPool_t *tp = malloc(sizeof(ThreadPool_t));
    tp->threads = malloc(num * sizeof(pthread_t));

    tp->num_workers = num;
    tp->active_workers = 0;
    tp->jobs.size = 0;
    tp->jobs.head = NULL;

    pthread_mutex_init(&tp->mutex, NULL);
    for (unsigned int i = 0; i < num; i++) {
        pthread_create(&tp->threads[i], NULL, (void *)Thread_run, tp);
    }

    return tp;
}


void ThreadPool_destroy(ThreadPool_t *tp) {
    ThreadPool_check(tp);
    pthread_mutex_destroy(&tp->mutex);
    for (unsigned int i = 0; i < tp->num_workers; i++) {
        pthread_join(tp->threads[i], NULL);
    }
    free(tp->threads);
    free(tp);
}


bool ThreadPool_add_job(ThreadPool_t *tp, thread_func_t func, void *arg) {
    struct stat job_st;
    if (stat((char *)arg, &job_st) != 0) {
        return false;       // Error reading the file
    }

    // Create a new job
    ThreadPool_job_t *job = malloc(sizeof(ThreadPool_job_t)); 
    job->func = func;
    job->arg = arg;

    // Insert the job in ascending order by file size
    ThreadPool_job_t **current = &tp->jobs.head;
    struct stat current_st;
    while (*current != NULL) {
        stat((char *)(*current)->arg, &current_st);
        if (current_st.st_size > job_st.st_size)
            break;
        current = &(*current)->next;
    }

    job->next = *current;
    *current = job;
    
    tp->jobs.size++;

    return true;
}


ThreadPool_job_t *ThreadPool_get_job(ThreadPool_t *tp) {
    if (tp->jobs.head == NULL)
        return NULL;
    ThreadPool_job_t *job = tp->jobs.head;
    tp->jobs.head = tp->jobs.head->next;
    tp->jobs.size--;
    return job;
}


void *Thread_run(ThreadPool_t *tp) {
    while (1) {
        pthread_mutex_lock(&tp->mutex);

        // Break the loop if there are no more jobs
        if (tp->jobs.size == 0 && tp->active_workers == 0) {
            pthread_mutex_unlock(&tp->mutex);
            break;
        }

        ThreadPool_job_t *job = ThreadPool_get_job(tp);

        if (job == NULL) {
            pthread_mutex_unlock(&tp->mutex);
        } else {
            tp->active_workers++;  // Increment active thread count
            pthread_mutex_unlock(&tp->mutex);

            job->func(job->arg);
            free(job);

            pthread_mutex_lock(&tp->mutex);
            tp->active_workers--;
            pthread_mutex_unlock(&tp->mutex);
        }
    }
    return NULL;
}


void ThreadPool_check(ThreadPool_t *tp) {
    ThreadPool_job_t *job = ThreadPool_get_job(tp);
    while (job != NULL) {
        ThreadPool_job_t *temp = job;
        job = ThreadPool_get_job(tp);
        free(temp);
    }
}
