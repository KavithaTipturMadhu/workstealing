//============================================================================
// Name        : workstealing-benchmark.cpp
// Author      : 
// Version     :
// Copyright   : Your copyright notice
//============================================================================

#include "mpi.h"
#include <stdio.h>
#include <pthread.h>
#include <stdlib.h>
#define N 20
#define QUEUE_SIZE 65536
#define THRESHOLD 2
#define STEAL_ATTEMPTS_THRESHOLD 5000

typedef struct task {
	int arg;
	int task_id;
} task;

typedef struct task_return {
	int arg;
	enum return_type {
		TASK, INTEGER
	} type;
	union {
		struct task_value {
			task* task_list;
			int num_tasks;
		} task_value;
		long value;
	} value;
} task_return;

void fib_function(void * thread_return_value) {
	int fib_count = ((struct task_return *) thread_return_value)->arg;
	double prev_value;
	struct task_return return_value;
	if (fib_count < THRESHOLD) {
		long fib_value;
		for (int i = 0; i <= fib_count; i++) {
			if (i == 0) {
				fib_value = 0;
			} else if (i == 1) {
				prev_value = fib_value;
				fib_value = 1;
			} else {
				double new_fib_value = fib_value + prev_value;
				prev_value = fib_value;
				fib_value = new_fib_value;
			}
		}
		return_value.type = INTEGER;
		return_value.value.value = fib_value;
	} else {
		task* children = (task*) calloc(2, sizeof(task));
		children[0].arg = fib_count - 1;
		children[0].task_id = 1;

		children[1].arg = fib_count - 2;
		children[1].task_id = 1;

		return_value.type = TASK;
		return_value.value.task_value.task_list = children;
		return_value.value.task_value.num_tasks = 2;
	}

	*((struct task_return*) thread_return_value) = return_value;
	pthread_exit(thread_return_value);
}

int main(int argc, char ** argv) {
	int rank, size;
	task * task_queue = (task *) calloc(QUEUE_SIZE, sizeof(task));
	/* Front pointer at 0, back pointer at 1, spin lock at 2*/
	int task_ptr[3];
	MPI_Win task_win, task_ptr_win;
	unsigned int seed;
	MPI_Request request;
	long sum = 0;

	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &size);

	int compare_value = -1;
	int lock_value = rank;
	int prev_value;
	int num_attempts = 0;

	//Initialize some tasks in each queue per process
	if (rank == 0) {
		struct task start_task;
		start_task.arg = N;
		start_task.task_id = 1;
		task_queue[0] = start_task;
		task_ptr[0] = 0;

		task_ptr[1] = 1;
		task_ptr[2] = -1;
	} else {
		task_ptr[0] = 0;
		task_ptr[1] = 0;
		task_ptr[2] = -1;
	}

	MPI_Win_create(&task_queue[0], QUEUE_SIZE * sizeof(struct task), sizeof(struct task), MPI_INFO_NULL,
	MPI_COMM_WORLD, &task_win);

	MPI_Win_create(&task_ptr[0], 3, sizeof(int), MPI_INFO_NULL,
	MPI_COMM_WORLD, &task_ptr_win);

//	MPI_Win_lock_all(MPI_MODE_NOCHECK, task_ptr_win);
	MPI_Win_lock_all(MPI_MODE_NOCHECK, task_win);

	int remoteflag[3] = { 0, 0, -1 };
	task current;
	current.arg = -1;

	while (1) {
		MPI_Barrier(MPI_COMM_WORLD);
		int global_num_attempts = num_attempts;
		MPI_Request request;
		MPI_Allreduce(&num_attempts, &global_num_attempts, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
		if (global_num_attempts > 0) {
			break;
		}
		task current;
		current.task_id = -1;

		int count = 0;
		int i = rank;
		while(count++<size) {
			int remoteflag[3] = { 0, 0, -1 };

			MPI_Win_lock_all(MPI_MODE_NOCHECK, task_ptr_win);
			MPI_Compare_and_swap(&lock_value, &compare_value, &prev_value, MPI_INT, i, 2, task_ptr_win);
			MPI_Get(&remoteflag[0], 3, MPI_INT, i, 0, 3, MPI_INT, task_ptr_win);
			MPI_Win_flush(i, task_ptr_win);
			MPI_Win_unlock_all(task_ptr_win);
			MPI_Win_fence(MPI_MODE_NOCHECK, task_ptr_win);

			if(remoteflag[2] == rank &&  current.task_id == -1 && remoteflag[0] != remoteflag[1] && remoteflag[1] != -1 && remoteflag[0] < QUEUE_SIZE) {
				MPI_Win_lock_all(MPI_MODE_NOCHECK, task_ptr_win);
				MPI_Get(&current, sizeof(struct task), MPI_BYTE, i, remoteflag[0], sizeof(struct task), MPI_BYTE, task_win);
				remoteflag[0] = (remoteflag[0] + 1) % QUEUE_SIZE;
				MPI_Accumulate(&remoteflag[0], 1,
				MPI_INT, i, 0, 1,
				MPI_INT, MPI_REPLACE, task_ptr_win);
				MPI_Win_flush(rank, task_ptr_win);
				MPI_Win_flush(rank, task_win);
				MPI_Win_unlock_all(task_ptr_win);
			}
			MPI_Win_fence(MPI_MODE_NOCHECK, task_ptr_win);

			MPI_Win_lock_all(MPI_MODE_NOCHECK, task_ptr_win);
			MPI_Compare_and_swap(&compare_value, &lock_value, &prev_value, MPI_INT, i, 2, task_ptr_win);
			MPI_Win_flush_all(task_ptr_win);
			MPI_Win_unlock_all(task_ptr_win);
			i = (i+1)%size;
		}
		MPI_Win_fence(MPI_MODE_NOCHECK, task_ptr_win);

		MPI_Win_lock_all(MPI_MODE_NOCHECK, task_ptr_win);
		MPI_Compare_and_swap(&lock_value, &compare_value, &prev_value, MPI_INT, rank, 2, task_ptr_win);
		MPI_Win_flush(rank, task_ptr_win);
		MPI_Win_unlock_all(task_ptr_win);
		MPI_Win_fence(MPI_MODE_NOCHECK, task_ptr_win);

		if (current.task_id >= 0) {
			pthread_t thread;
			struct task_return *return_value = malloc(sizeof(struct task_return));
			return_value->arg = current.arg;
			pthread_create(&thread, NULL, (void*) fib_function, return_value);
			pthread_join(thread, NULL);
			if (return_value->type == INTEGER) {
//				printf("fib value computed as %ld\n", return_value->value.value);
				sum += return_value->value.value;
			} else if(task_ptr[2] == rank){
				/* Queue the tasks in the local task queue */
				for (int i = 0; i < return_value->value.task_value.num_tasks; i++) {
					task_queue[task_ptr[1]] = return_value->value.task_value.task_list[i];
					task_ptr[1] = (task_ptr[1] + 1) % QUEUE_SIZE;
					if(task_ptr[1] == task_ptr[0]){
						MPI_Abort(MPI_COMM_WORLD,  MPI_ERR_OTHER);
					}
				}

			}
			num_attempts = 0;
		} else {
			num_attempts++;
		}

		MPI_Win_lock_all(MPI_MODE_NOCHECK, task_ptr_win);
		MPI_Compare_and_swap(&compare_value, &lock_value, &prev_value, MPI_INT, rank, 2, task_ptr_win);
		MPI_Win_flush(rank, task_ptr_win);
		MPI_Win_unlock_all(task_ptr_win);
		MPI_Win_fence(MPI_MODE_NOCHECK, task_ptr_win);
		MPI_Barrier(MPI_COMM_WORLD);
	}
	MPI_Win_unlock_all(task_win);
	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Allreduce(MPI_IN_PLACE, &sum, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
	printf("global sum:%ld\n", sum);
	MPI_Win_free(&task_win);
	MPI_Win_free(&task_ptr_win);
	free(task_queue);
	return 0;
}