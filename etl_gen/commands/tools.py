import csv
from kafka import KafkaProducer
from typing import Callable
import time
import os

import typer

from rich.console import Console


err_console = Console(stderr=True)


def batch(func, num, col_name, path, writemode):
    try:
        buffer = []
        with open(path, writemode, encoding="utf8", newline="") as f:
            csv_f = csv.writer(f)
            csv_f.writerow([col_name])
            for i in range(int(num)):
                row = func()
                buffer.append([row])
                if len(buffer) == 1000:
                    csv_f.writerows(buffer)
                    buffer.clear()
            csv_f.writerows(buffer)
            print(buffer)
    except PermissionError as e:
        pass


def check_path_is_empty_file(path: str):
    if os.path.exists(path):
        err_console.print(path + "已存在")
        typer.Exit(1)


class StreamToKafka:

    def __init__(self, bootstrap_servers: str, topic: str, delay: int, func: Callable[..., str]):
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
        self.topic = topic
        self.delay = delay
        self.func = func

    def run(self):
        while True:
            self.producer.send(self.topic,self.func())
            time.sleep(2)


class WriteRowFile:

    def __init__(self,file_path:str,num:int,func):
        if os.path.exists(file_path):
            err_console.print(file_path + "已经存在")
            typer.Exit(1)

        self.file_path = file_path
        self.num = num
        self.func = func

    def run(self):
        with open(self.file_path, 'w', encoding='utf8', newline="") as f:
            for i in range(0, self.num):
                f.write(self.func())
                if i != self.num - 1:
                    f.write("\n")