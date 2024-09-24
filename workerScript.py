from dask.distributed import LocalCluster
from prefect import Flow, task, context
from prefect.executors import DaskExecutor
from prefect.engine.signals import FAIL
from datetime import timedelta
from bot_handle import flow_success_handler, flow_failure_handler

# Создаем локальный кластер Dask с ограничением ресурсов
cluster = LocalCluster(
    n_workers=2,            # Количество worker'ов
    threads_per_worker=1,    # Количество потоков на worker
    memory_limit="1GB"       # Лимит памяти на worker
)

# Создаем DaskExecutor с подключением к кластеру
executor = DaskExecutor(cluster)

# Задача с тайм-аутом, повторными попытками и логированием
@task(timeout_seconds=30, retries=3, retry_delay=timedelta(seconds=10))
def limited_task():
    # Получаем logger из контекста Prefect
    logger = context.get("logger")
    
    try:
        logger.info("Задача начата")
        
        import time
        logger.info("Имитация длительного выполнения задачи...")
        time.sleep(35)  # Задача, которая должна превысить лимит времени
        
        logger.info("Задача успешно выполнена")
        return "Task completed"
    
    except Exception as e:
        # Логируем ошибку
        logger.error(f"Ошибка в задаче: {e}")
        # Прерываем задачу с сигналом FAIL, чтобы Prefect использовал retries
        raise FAIL("Ошибка: Тайм-аут или другое исключение")

# Создаем поток
with Flow("dask-flow-with-logging-and-retries") as flow:
    result = limited_task()

flow.set_reference_tasks([result])#Привязка колбэков к состоянию потока
flow.on_success(flow_success_handler)
flow.on_failure(flow_failure_handler)

# Запуск потока с использованием DaskExecutor
if __name__ == "__main__":
    # Логирование запуска потока
    logger = context.get("logger")
    logger.info("Запуск потока...")
    flow.run(executor=executor)
    logger.info("Поток завершен")
