import os
import psutil
import time
import threading

def memory_benchmark(func):
    def wrapper(*args, **kwargs):
        process = psutil.Process(os.getpid())
        mem_samples = []
        stop_flag = False

        def benchmark():
            while not stop_flag:
                mem = process.memory_info().rss / 1024 / 1024
                mem_samples.append(mem)
                time.sleep(0.01)
                
        benchmark_worker = threading.Thread(target=benchmark)
        benchmark_worker.start()

        try:
            output = func(*args, **kwargs)
        finally:
            stop_flag = True
            benchmark_worker.join()

        print(f"{func.__name__} used min: {min(mem_samples)}MB, max: {max(mem_samples)}MB, avg: {sum(mem_samples) / len(mem_samples) if mem_samples else 0}")


        return output      
    return wrapper