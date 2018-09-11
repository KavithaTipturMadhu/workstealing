//============================================================================
// Name        : workstealing-benchmark.cpp
// Author      : 
// Version     :
// Copyright   : Your copyright notice
//============================================================================

#include "mpi.h"
#include <stdio.h>
#include <stdlib.h>
//#define N 30
#define THRESHOLD 2
#define TERMINATION_THRESHOLD 1000
#define QUEUE_SIZE (65536*1024)
#define MIN 10
#define MAX 20

typedef struct task {
	char arg;
	char valid;
} task;

typedef struct task_return {
	enum return_type {
		TASK, IMMEDIATE
	} type;
	union {
		struct task_value {
			task* task_list;
			int num_tasks;
		} task_value;
		long value;
	} value;
} task_return;

void fib_function(int arg, struct task_return* function_return_value) {
	double prev_value;
	struct task_return return_value;
	if (arg < THRESHOLD) {
		long fib_value;
		for (int i = 0; i <= arg; i++) {
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
		return_value.type = IMMEDIATE;
		return_value.value.value = fib_value;
	} else {
		task* children = (task*) calloc(2, sizeof(task));
		children[0].arg = arg - 1;
		children[0].valid = 1;

		children[1].arg = arg - 2;
		children[1].valid = 1;

		return_value.type = TASK;
		return_value.value.task_value.task_list = children;
		return_value.value.task_value.num_tasks = 2;
	}

	*((struct task_return*) function_return_value) = return_value;
}

int main(int argc, char ** argv) {
	int rank, size;
	task * task_queue = (task *) calloc(QUEUE_SIZE, sizeof(task));
	/* Front pointer at 0, back pointer at 1, spin lock at 2, attempts in 3*/
	int task_ptr[3];
	MPI_Win task_win, task_ptr_win;
	unsigned int seed;
	MPI_Request request;

	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &size);
	int compare_value = -1;
	int lock_value = rank;
	int prev_value;
	int num_attempts = 0;
	int num_tasks = 0;
	int N;

	MPI_Win_create(&task_queue[0], QUEUE_SIZE, sizeof(struct task), MPI_INFO_NULL,
	MPI_COMM_WORLD, &task_win);

	MPI_Win_create(&task_ptr[0], 3, sizeof(int), MPI_INFO_NULL,
	MPI_COMM_WORLD, &task_ptr_win);

	srand(time(0));
	for (N = MIN; N < MAX; N++) {
		long sum = 0;
		MPI_Win_lock_all(MPI_MODE_NOCHECK, task_win);
		//Initialize some tasks in each queue per process
		if (rank == 0) {
			struct task start_task;
			start_task.arg = N;
			start_task.valid = 1;
			task_queue[0] = start_task;
			task_ptr[0] = 0;
			task_ptr[1] = 1;
			//	} else if(rank == size/2){
			//		struct task start_task;
			//		start_task.arg = N - 2;
			//		start_task.valid = 1;
			//		task_queue[0] = start_task;
			//		task_ptr[0] = 0;
			//		task_ptr[1] = 1;
		} else {
			task_ptr[0] = 0;
			task_ptr[1] = 0;
		}
		task_ptr[2] = -1;
		int remoteflag[3] = { 0, 0, -1 };
		task current;
		current.arg = -1;
		int* num_steals = calloc(size, sizeof(int));
		int num_iterations = 0;
		int sync_freq = 1;
		double start_time = MPI_Wtime();
		int victim = (rank + 1)%size;
		while (1) {
			task current;
			current.valid = -1;
			if (num_iterations >= (TERMINATION_THRESHOLD)) {
				int global_attempts;
				MPI_Allreduce(&num_attempts, &global_attempts, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
				if (global_attempts > 0) {
					break;
				}
				num_iterations = 0;
			}

			MPI_Win_lock(MPI_LOCK_SHARED, rank, MPI_MODE_NOCHECK, task_ptr_win);
			MPI_Compare_and_swap(&lock_value, &compare_value, &prev_value, MPI_INT, rank, 2, task_ptr_win);
			if (task_ptr[2] == rank && task_ptr[0] != task_ptr[1] && task_ptr[1] != -1 && task_ptr[0] < QUEUE_SIZE) {
				int tail = (task_ptr[1] - 1) % QUEUE_SIZE;
				current = task_queue[tail];
				task_ptr[1] = tail;
			}
			MPI_Compare_and_swap(&compare_value, &lock_value, &prev_value, MPI_INT, rank, 2, task_ptr_win);
			MPI_Win_flush_local(rank, task_ptr_win);
			MPI_Win_unlock(rank, task_ptr_win);

			if (current.valid == -1) {
				int i = rand() % size;
//				int i = victim;
				victim = (victim + 1)%size;
				if(victim == rank){
					victim = (victim + 1)%size;
				}
				int remoteflag[3] = { 0, 0, -1 };
				MPI_Win_lock(MPI_LOCK_SHARED, i, MPI_MODE_NOCHECK, task_ptr_win);
				MPI_Compare_and_swap(&lock_value, &compare_value, &prev_value, MPI_INT, i, 2, task_ptr_win);
				MPI_Get(&remoteflag[0], 3, MPI_INT, i, 0, 3, MPI_INT, task_ptr_win);
				MPI_Win_flush_local(i, task_ptr_win);

				if (remoteflag[2] == rank && current.valid == -1 && remoteflag[0] != remoteflag[1] && remoteflag[1] != -1 && remoteflag[0] < QUEUE_SIZE) {
					MPI_Get(&current, sizeof(struct task), MPI_BYTE, i, remoteflag[0], sizeof(struct task), MPI_BYTE, task_win);
					remoteflag[0] = (remoteflag[0] + 1) % QUEUE_SIZE;
					MPI_Put(&remoteflag[0], 1,
					MPI_INT, i, 0, 1,
					MPI_INT, task_ptr_win);
					num_steals[i]++;
				}

				/* Replace with fetch and op */
				MPI_Compare_and_swap(&compare_value, &lock_value, &prev_value, MPI_INT, i, 2, task_ptr_win);
				MPI_Win_unlock(i, task_ptr_win);
			}

			if (current.valid >= 0) {
				struct task_return *return_value = malloc(sizeof(struct task_return));
				num_tasks++;
				fib_function(current.arg, return_value);
				if (return_value->type == IMMEDIATE) {
					sum += return_value->value.value;
				} else {
					/* Queue the tasks in the local task queue */
					for (int i = 0; i < return_value->value.task_value.num_tasks; i++) {
						task_queue[task_ptr[1]] = return_value->value.task_value.task_list[i];
						task_ptr[1] = (task_ptr[1] + 1) % QUEUE_SIZE;
						if (task_ptr[1] == task_ptr[0]) {
							MPI_Abort(MPI_COMM_WORLD, MPI_ERR_OTHER);
						}
					}
				}
				num_attempts = 0;
			} else {
				num_attempts++;
			}
			num_iterations++;
		}
		MPI_Win_unlock_all(task_win);
		int global_sum;
		MPI_Allreduce(&sum, &global_sum, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
		if (rank == 0) {
			printf("N:%d %f\n", global_sum, (MPI_Wtime() - start_time));
		}
//		printf("rank %d, partial sum: %d, steals:", rank, sum);
//		for (int i = 0; i < size; i++) {
//			printf("%d, ", num_steals[i]);
//		}
//		printf("\n");
		fflush(stdout);
	}
	MPI_Win_free(&task_win);
	MPI_Win_free(&task_ptr_win);
	free(task_queue);

//	printf("\n");
	fflush(stdout);

	MPI_Finalize();
	return 0;
}
