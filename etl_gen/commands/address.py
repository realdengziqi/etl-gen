import os.path
import time

import typer
import random
import json

from etl_gen.commands.tools import WriteRowFile, StreamToKafka

app_address = typer.Typer(
    name="address",
    help="生成不规范的中文地址"
)

address_list = []


def get_address_json():
    current_dir = os.path.dirname(__file__)
    f = open(os.path.join(current_dir, "../resources/mixed_address.txt"), 'r', encoding="utf-8")
    data_list = f.readlines()
    for i in data_list:
        one = json.loads(i)
        address_list.append(one["query"])


def get_one_address():
    return random.choice(address_list)





@app_address.command(help="创建一个文件，并向里面写入生成的数据")
def batch(
        num: int = typer.Option(..., help="文件中包含的数据行数", min=50, rich_help_panel="参数"),
        file_path: str = typer.Option(..., help="要生成的文件路径，此文件之前不能存在", rich_help_panel="参数", show_default=False)
):
    get_address_json()
    wf = WriteRowFile(file_path, num, get_one_address)
    wf.run()


@app_address.command(help="向Kafka持续生产数据")
def stream(bootstrap_servers: str = typer.Option(..., help="Kafka服务地址", show_default=False, rich_help_panel="参数"),
           topic: str = typer.Option(..., help="Kafka主题", show_default=False, rich_help_panel="参数"),
           delay: int = typer.Option(1, help="发送数据的间隔的秒数", rich_help_panel="参数")
           ):
    get_address_json()
    producer = StreamToKafka(bootstrap_servers, topic, delay, get_one_address)
    producer.run()


@app_address.command(help="向控制台每隔一段时间输出一条数据")
def test(delay: int = typer.Option(1, help="间隔秒数")):
    get_address_json()
    while True:
        print(get_one_address(), flush=True)
        time.sleep(delay)
