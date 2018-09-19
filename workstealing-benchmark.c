//============================================================================
// Name        : workstealing-benchmark.cpp
// Author      : 
// Version     :
// Copyright   : Your copyright notice
//============================================================================

#include "mpi.h"
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
//#define N 30
#define THRESHOLD 2
#define LOCAL_STEAL_THRESHOLD 5
#define QUEUE_SIZE 65536
#define LOCAL_QUEUE_SIZE 32
#define MIN 40
#define MAX 41
#define QUEUE_FULL(head, tail , queue_size) (tail>=0 && ((head > tail && head == (tail+1)%queue_size)||(head == 0 && tail == queue_size - 1)))
#define QUEUE_EMPTY(head, tail) (head == 0 && tail == -1)

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
	int rank, size, local_rank, local_size;
	task * task_queue = (task *) calloc(QUEUE_SIZE, sizeof(task));
	/* Front pointer at 0, back pointer at 1, spin lock at 2, attempts in 3*/
	int task_ptr[3];

	task * local_task_queue = (task *) calloc(LOCAL_QUEUE_SIZE, sizeof(task));
	int local_task_ptr[3];

	MPI_Win task_win, task_ptr_win;
	MPI_Win local_task_win, local_task_ptr_win;
	unsigned int seed;
	MPI_Request request;
	MPI_Info split_info;
	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &size);
	int compare_value = -1;
	int lock_value = rank;
	int prev_value;
	int num_global_attempts = 0, num_local_attempts = 0;
	int num_tasks = 0;
	int N;

	MPI_Win_create(&task_queue[0], QUEUE_SIZE, sizeof(struct task), MPI_INFO_NULL,
	MPI_COMM_WORLD, &task_win);

	MPI_Win_create(&task_ptr[0], 3, sizeof(int), MPI_INFO_NULL,
	MPI_COMM_WORLD, &task_ptr_win);

	/* Create local queues */
	MPI_Info_create(&split_info);
	MPI_Info_set(split_info, "shmem_topo", "numa");
	MPI_Comm local_comm;
	MPI_Comm_split_type(MPI_COMM_WORLD, MPI_COMM_TYPE_SHARED, rank, split_info, &local_comm);
	MPI_Comm_size(local_comm, &local_size);
	MPI_Comm_rank(local_comm, &local_rank);
	int local_lock_value = local_rank;

	MPI_Win_create(&local_task_queue[0], LOCAL_QUEUE_SIZE, sizeof(struct task), MPI_INFO_NULL, local_comm, &local_task_win);

	MPI_Win_create(&local_task_ptr[0], 3, sizeof(int), MPI_INFO_NULL, local_comm, &local_task_ptr_win);

	srand(time(0));
	for (N = MIN; N < MAX; N++) {
		long sum = 0;
		MPI_Win_lock_all(MPI_MODE_NOCHECK, task_win);
		MPI_Win_lock_all(MPI_MODE_NOCHECK, local_task_win);
		//Initialize some tasks in each queue per process
		if (rank == 0) {
			struct task start_task;
			start_task.arg = N;
			start_task.valid = 1;
			local_task_queue[0] = start_task;
			local_task_ptr[0] = 0;
			local_task_ptr[1] = 0;
		} else {
			local_task_ptr[0] = 0;
			local_task_ptr[1] = -1;
		}
		local_task_ptr[2] = -1;
		task_ptr[0] = 0;
		task_ptr[1] = -1;
		task_ptr[2] = -1;

		task current;
		current.arg = -1;
		int* num_local_steals = calloc(local_size, sizeof(int));
		int* num_global_steals = calloc(size, sizeof(int));
		int num_iterations = 0;
		int sync_freq = 1;
		double start_time = MPI_Wtime();
		while (1) {
			task current;
			current.valid = -1;
			if (num_iterations >= ((pow(2, N+1)/size))) {
				int attempts = (num_global_attempts && (QUEUE_EMPTY(task_ptr[0], task_ptr[1]))&&(QUEUE_EMPTY(local_task_ptr[0], local_task_ptr[1])));
				MPI_Allreduce(MPI_IN_PLACE, &attempts, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
				if (attempts > 0) {
//					printf("state of queue in rank %d: local %d, %d, global %d %d, num attempts %d\n",rank, local_task_ptr[0], local_task_ptr[1], task_ptr[0], task_ptr[1], num_global_attempts);
					break;
				}
				num_iterations = 0;
			}

			/* Check in local queue for work */
			MPI_Win_lock(MPI_LOCK_SHARED, local_rank, MPI_MODE_NOCHECK, local_task_ptr_win);
			MPI_Compare_and_swap(&local_lock_value, &compare_value, &prev_value, MPI_INT, local_rank, 2, local_task_ptr_win);
			if (local_task_ptr[2] == local_rank && !QUEUE_EMPTY(local_task_ptr[0], local_task_ptr[1])) {
				current = local_task_queue[local_task_ptr[1]];
				if (local_task_ptr[1] == local_task_ptr[0]) {
					local_task_ptr[0] = 0;
					local_task_ptr[1] = -1;
				} else {
					local_task_ptr[1] = (local_task_ptr[1] - 1 + LOCAL_QUEUE_SIZE) % LOCAL_QUEUE_SIZE;
				}
			}
			MPI_Compare_and_swap(&compare_value, &local_lock_value, &prev_value, MPI_INT, local_rank, 2, local_task_ptr_win);
			MPI_Win_flush_local(local_rank, local_task_ptr_win);
			MPI_Win_unlock(local_rank, local_task_ptr_win);

			/* Check in global queue for work */
			if (current.valid == -1) {
				MPI_Win_lock(MPI_LOCK_SHARED, rank, MPI_MODE_NOCHECK, task_ptr_win);
				MPI_Compare_and_swap(&lock_value, &compare_value, &prev_value, MPI_INT, rank, 2, task_ptr_win);
				if (task_ptr[2] == rank && !QUEUE_EMPTY(task_ptr[0], task_ptr[1])) {
					current = task_queue[task_ptr[1]];
					if (task_ptr[1] == task_ptr[0]) {
						task_ptr[0] = 0;
						task_ptr[1] = -1;
					} else {
						task_ptr[1] = (task_ptr[1] - 1 + QUEUE_SIZE) % QUEUE_SIZE;
					}
				}
				MPI_Compare_and_swap(&compare_value, &lock_value, &prev_value, MPI_INT, rank, 2, task_ptr_win);
				MPI_Win_flush_local(rank,task_ptr_win);
				MPI_Win_unlock(rank, task_ptr_win);
			}

			/* Steal work from local communicator */
			if (current.valid == -1 && num_local_attempts < LOCAL_STEAL_THRESHOLD) {
				int i = rand() % local_size;
				int remoteflag[3] = { 0, -1, -1 };
				MPI_Win_lock(MPI_LOCK_SHARED, i, MPI_MODE_NOCHECK, local_task_ptr_win);
				if (i != local_rank) {
					MPI_Compare_and_swap(&local_lock_value, &compare_value, &prev_value, MPI_INT, i, 2, local_task_ptr_win);
					MPI_Get(&remoteflag[0], 3, MPI_INT, i, 0, 3, MPI_INT, local_task_ptr_win);
					MPI_Win_flush_local(i, local_task_ptr_win);

					if (remoteflag[2] == local_rank && current.valid == -1 && !QUEUE_EMPTY(remoteflag[0], remoteflag[1])) {
						MPI_Get(&current, sizeof(struct task), MPI_BYTE, i, remoteflag[0], sizeof(struct task), MPI_BYTE, local_task_win);
						if (remoteflag[0] == remoteflag[1]) {
							remoteflag[0] = 0;
							remoteflag[1] = -1;
						} else {
							remoteflag[0] = (remoteflag[0] + 1) % LOCAL_QUEUE_SIZE;
						}
						MPI_Put(&remoteflag[0], 2,
						MPI_INT, i, 0, 2,
						MPI_INT, local_task_ptr_win);
						num_local_steals[i]++;
					}
					MPI_Compare_and_swap(&compare_value, &local_lock_value, &prev_value, MPI_INT, i, 2, local_task_ptr_win);
					MPI_Win_flush_all(local_task_ptr_win);
					MPI_Win_flush_all(local_task_win);		
				}
				MPI_Win_unlock(i, local_task_ptr_win);
			}

			if (current.valid == -1 && num_local_attempts < LOCAL_STEAL_THRESHOLD) {
				num_local_attempts++;
			}

			int global_attempt_made = 0;
			/* Steal work from global communicator */
			if (current.valid == -1 && num_local_attempts >= LOCAL_STEAL_THRESHOLD) {
				global_attempt_made = 1;
				num_local_attempts = 0;
				int i = rand() % size;
				MPI_Win_lock(MPI_LOCK_SHARED, i, MPI_MODE_NOCHECK, task_ptr_win);
				if (i != rank) {
					int remoteflag[3] = { 0, -1, -1 };
					MPI_Compare_and_swap(&lock_value, &compare_value, &prev_value, MPI_INT, i, 2, task_ptr_win);
					MPI_Get(&remoteflag[0], 3, MPI_INT, i, 0, 3, MPI_INT, task_ptr_win);
					MPI_Win_flush_local(i, task_ptr_win);
					if (remoteflag[2] == rank && current.valid == -1 && !QUEUE_EMPTY(remoteflag[0], remoteflag[1])) {
						MPI_Get(&current, sizeof(struct task), MPI_BYTE, i, remoteflag[0], sizeof(struct task), MPI_BYTE, task_win);
						if (remoteflag[0] == remoteflag[1]) {
							remoteflag[0] = 0;
							remoteflag[1] = -1;
						} else {
							remoteflag[0] = (remoteflag[0] + 1) % QUEUE_SIZE;
						}
						MPI_Put(&remoteflag[0], 2,
						MPI_INT, i, 0, 2,
						MPI_INT, task_ptr_win);
						num_global_steals[i]++;
					}

					/* Replace with fetch and op */
					MPI_Compare_and_swap(&compare_value, &lock_value, &prev_value, MPI_INT, i, 2, task_ptr_win);
					MPI_Win_flush_all( task_ptr_win);
					MPI_Win_flush_all(task_win);
				}
				MPI_Win_unlock(i, task_ptr_win);
			}

			if (current.valid == -1 && global_attempt_made) {
				num_global_attempts++;
			}

			MPI_Win_fence(MPI_MODE_NOCHECK, task_ptr_win);
			MPI_Win_fence(MPI_MODE_NOCHECK, local_task_ptr_win);

			if (current.valid >= 0) {
				struct task_return *return_value = malloc(sizeof(struct task_return));
				num_tasks++;
				fib_function(current.arg, return_value);
				if (return_value->type == IMMEDIATE) {
					sum += return_value->value.value;
				} else {
					MPI_Win_lock(MPI_LOCK_SHARED, local_rank, MPI_MODE_NOCHECK, local_task_ptr_win);
					MPI_Win_lock(MPI_LOCK_SHARED, rank, MPI_MODE_NOCHECK, task_ptr_win);		
					/* Queue the tasks in the local task queue */
					while (local_task_ptr[2] != local_rank) {
						MPI_Compare_and_swap(&local_lock_value, &compare_value, &prev_value, MPI_INT, local_rank, 2, local_task_ptr_win);
					}
					while (task_ptr[2] != rank) {
						MPI_Compare_and_swap(&lock_value, &compare_value, &prev_value, MPI_INT, rank, 2, task_ptr_win);
					}

					for (int i = 0; i < return_value->value.task_value.num_tasks; i++) {
						int insert_position;
						if (QUEUE_FULL(local_task_ptr[0], local_task_ptr[1], LOCAL_QUEUE_SIZE)) {
							if (QUEUE_FULL(task_ptr[0], task_ptr[1], QUEUE_SIZE)) {
//								printf("aborting cos local queue %d %d, global queue %d %d\n", local_task_ptr[0], local_task_ptr[1], task_ptr[0], task_ptr[1]);
//								fflush(stdout);
								MPI_Abort(MPI_COMM_WORLD, MPI_ERR_OTHER);
							}
//							printf("at least one global queue enqueue\n");
							task_ptr[1] = (task_ptr[1] + 1) % QUEUE_SIZE;
							task_queue[task_ptr[1]] = return_value->value.task_value.task_list[i];
						} else {
							local_task_ptr[1] = (local_task_ptr[1] + 1) % LOCAL_QUEUE_SIZE;
							local_task_queue[local_task_ptr[1]] = return_value->value.task_value.task_list[i];
						}
					}

					MPI_Compare_and_swap(&compare_value, &local_lock_value, &prev_value, MPI_INT, local_rank, 2, local_task_ptr_win);
					MPI_Compare_and_swap(&compare_value, &lock_value, &prev_value, MPI_INT, rank, 2, task_ptr_win);
					MPI_Win_flush_local(rank, task_ptr_win);
					MPI_Win_flush_local(local_rank, local_task_ptr_win);
					MPI_Win_unlock(rank, task_ptr_win);
					MPI_Win_unlock(local_rank, local_task_ptr_win);
				}
				num_global_attempts = 0;
				num_local_attempts = 0;
			}
			MPI_Win_fence(MPI_MODE_NOCHECK, task_ptr_win);
			MPI_Win_fence(MPI_MODE_NOCHECK, local_task_ptr_win);
			num_iterations++;
		}
		MPI_Win_unlock_all(task_win);
		MPI_Win_unlock_all(local_task_win);
		MPI_Barrier(local_comm);
		MPI_Barrier(MPI_COMM_WORLD);
		int global_sum;
		MPI_Allreduce(&sum, &global_sum, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
		if (rank == 0) {
			printf("N:%d %f\n", global_sum, (MPI_Wtime() - start_time));
		}
//		printf("rank %d, partial sum: %d, num tasks %d, local queue %d %d, global queue %d %d\n", rank, sum, num_tasks, local_task_ptr[0], local_task_ptr[1], task_ptr[0], task_ptr[1]);
	//	for (int i = 0; i < size; i++) {
	//		printf("%d, ", num_global_steals[i]);
	//	}
	//	printf("\n");
	}
	MPI_Barrier(local_comm);
	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Win_free(&task_win);
	MPI_Win_free(&task_ptr_win);
	MPI_Win_free(&local_task_win);
	MPI_Win_free(&local_task_ptr_win);
	free(task_queue);
	free(local_task_queue);
//	printf("\n");
	fflush(stdout);

	MPI_Finalize();
	return 0;
}
