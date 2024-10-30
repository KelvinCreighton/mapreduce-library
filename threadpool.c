#include <stdlib.h>
#include <sys/stat.h>
#include "threadpool.h"




/**
* C style constructor for creating a new ThreadPool object
* Parameters:
*     num - Number of threads to create
* Return:
*     ThreadPool_t* - Pointer to the newly created ThreadPool object
*/
ThreadPool_t *ThreadPool_create(unsigned int num) {
	ThreadPool_t *threadPool = malloc(sizeof(ThreadPool_t));
	threadPool->threads = malloc(num*sizeof(pthread_t));

	threadPool->jobs.size = 0;
	threadPool->jobs.head = NULL;

	for (unsigned int i = 0; i < num; i++) {
        	pthread_create(&threadPool->threads[i], NULL, (void *)Thread_run, threadPool);
	}
	
	return threadPool;
}

/**
* C style destructor to destroy a ThreadPool object
* Parameters:
*     tp - Pointer to the ThreadPool object to be destroyed
*/
void ThreadPool_destroy(ThreadPool_t *tp) {

}

/**
* Add a job to the ThreadPool's job queue
* Parameters:
*     tp   - Pointer to the ThreadPool object
*     func - Pointer to the function that will be called by the serving thread
*     arg  - Arguments for that function
* Return:
*     true  - On success
*     false - Otherwise
*/
bool ThreadPool_add_job(ThreadPool_t *tp, thread_func_t func, void *arg) {
    ThreadPool_job_t *newJob = malloc(sizeof(ThreadPool_job_t));
    newJob->func = func;
    newJob->arg = arg;
    newJob->next = NULL;

    // REPLACE this with a minheap
	if (tp->jobs.size == 0) {
        tp->jobs.head = newJob;
    } else {
        // This is bad
        ThreadPool_job_t *currentJob = tp->jobs.head;
        while (currentJob->next != NULL) {
            currentJob = currentJob->next;
        }
        currentJob->next = newJob;
    }

    tp->jobs.size++; 
    return 1;
}

/**
* Get a job from the job queue of the ThreadPool object
* Parameters:
*     tp - Pointer to the ThreadPool object
* Return:
*     ThreadPool_job_t* - Next job to run
*/
ThreadPool_job_t *ThreadPool_get_job(ThreadPool_t *tp) {
	if (tp->jobs.head == NULL) {
        return NULL;
    }
    ThreadPool_job_t *nextJob = tp->jobs.head;
    tp->jobs.head = nextJob->next;  // Set the head to the next element
    tp->jobs.size -= 1;
    return nextJob;
}

/**
* Start routine of each thread in the ThreadPool Object
* In a loop, check the job queue, get a job (if any) and run it
* Parameters:
*     tp - Pointer to the ThreadPool object containing this thread
*/
void *Thread_run(ThreadPool_t *tp) {
	return NULL;
}

/**
* Ensure that all threads are idle and the job queue is empty before returning
* Parameters:
*     tp - Pointer to the ThreadPool object that will be destroyed
*/
void ThreadPool_check(ThreadPool_t *tp) {

}

