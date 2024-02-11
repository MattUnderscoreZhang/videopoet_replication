from multiprocessing import get_context
from tqdm import tqdm
from typing import Callable, Generator, Any


def retry_failed_shards(
    run_func: Callable[[list[Any]], list[Any]],
    failed_shards: list[Any],
    max_shard_retry: int,
) -> None:
    """
    Retry failed shards a maximum number of times.

    Args:
        run_func: The function to run for retrying failed shards.
        failed_shards: A list of failed shards.
        max_shard_retry: The maximum number of times to retry.

    Returns:
        None
    """
    for i in range(max_shard_retry):
        if len(failed_shards) == 0:
            break
        print(f"Retrying {len(failed_shards)} shards, try {i+1}")
        failed_shards = run_func(failed_shards)
    if len(failed_shards) != 0:
        print(
            f"Retried {max_shard_retry} times, but {len(failed_shards)} shards "
            "still failed. You may restart the same command to retry again."
        )


def multiprocessing_distributor(
    processes_count: int,
    worker: Callable[[Any], Any],
    input_sharder: Generator[Any, None, None],
    max_shard_retry: int
) -> None:
    """
    Distribute the work to the processes using multiprocessing.

    Args:
        processes_count: The number of processes to use.
        worker: The worker function to execute.
        input_sharder: The generator for input shards.
        max_shard_retry: The maximum number of times to retry failed shards.

    Returns:
        None
    """
    ctx = get_context("spawn")
    with ctx.Pool(processes_count, maxtasksperchild=5) as process_pool:

        def run(gen: Generator[Any, None, None]) -> list[Any]:
            failed_shards = []
            for status, row in tqdm(process_pool.imap_unordered(worker, gen)):
                if status is False:
                    failed_shards.append(row)
            return failed_shards

        failed_shards = run(input_sharder)
        retry_failed_shards(run, failed_shards, max_shard_retry)

        process_pool.terminate()
        process_pool.join()
        del process_pool