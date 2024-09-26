import concurrent.futures
import os
import psutil
from prefect.states import Completed, Failed
from prefect.task_runners import ConcurrentTaskRunner
from prefect import task, flow, context

from pandas import read_csv

from datetime import timedelta
from main import main, read_csv
from bot_handle import flow_success_handler, flow_failure_handler
from config import CSV_PATH

def limit_worker_resources():
    p = psutil.Process(os.getpid())
    p.cpu_affinity([0])
    p.nice(psutil.HIGH_PRIORITY_CLASS)
    
    try:
        p.rlimit(psutil.RLIMIT_AS, (2 * 1024 * 1024 * 1024, psutil.RLIM_INFINITY))  # Лимит 2 ГБ(unix)
    except AttributeError:
        pass  


# Задача с тайм-аутом, повторными попытками и логированием
@task(timeout_seconds=30, retries=3, retry_delay=timedelta(seconds=10))
def limited_task():
    logger = context.get("logger")
    limit_worker_resources()
    try:
        logger.info("Задача начата")
        main()
        logger.info("Задача успешно выполнена")
        return "Task completed"
    
    except Exception as e:
        logger.error(f"Ошибка в задаче: {e}")


def parallel_processing():
    data = read_csv(CSV_PATH)
    data_dict = data.to_dict(orient='records')
    # Используем ThreadPoolExecutor для выполнения задач в нескольких потоках
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        future_tasks = [executor.submit(limited_task.run, data) for data in data_dict]
        results = [future.result() for future in concurrent.futures.as_completed(future_tasks)]
    return results

def task_callback(task, old_state, new_state):#колбеки для отправки сообщения в телеграмм 
    if isinstance(new_state, Completed):
        flow_success_handler(task, new_state)
    elif isinstance(new_state, Failed):
        flow_failure_handler(task, new_state)
    return new_state

# Создаем поток
@flow(name="Parallel-Processing-Flow", task_runner=ConcurrentTaskRunner())
def main_flow():
    data_list = [1, 2, 3, 4, 5]
    results = []
    for data in data_list:
        # Привязываем колбэк к каждой задаче
        task_state = limited_task.with_options(on_completion=task_callback)(data)
        results.append(task_state)
    return results

    

if __name__ == "__main__":
    logger = context.get("logger")
    logger.info("Запуск потока...")
    main_flow.run().deploy(
        name="my-first-deployment",
        work_pool_name="my-pool", # Work pool target
        cron="0 1 * * *", # Cron schedule (1am every day)
    )
    logger.info("Поток завершен")
