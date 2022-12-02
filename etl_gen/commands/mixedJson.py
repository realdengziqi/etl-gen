import time

import typer
from etl_gen.commands.tools import WriteRowFile, StreamToKafka
import random
import os

app_json = typer.Typer(
    name="json",
    help="""
    生成包含地址信息的json格式数据
    """
)

data = []


def read_json_file():
    current_path = os.path.dirname(__file__)

    with open(os.path.join(current_path, "../resources/address_data"), "r", encoding="utf8") as f:
        for i in f.readlines():
            data.append(i)


def get_one_json():
    return random.choice(data)





@app_json.command(help="创建一个文件，并向里面写入生成的数据")
def batch(
        num: int = typer.Option(20, help="文件中包含的数据行数", min=50, rich_help_panel="参数"),
        file_path: str = typer.Option(..., help="要生成的文件路径，此文件之前不能存在", rich_help_panel="参数", show_default=False)
):
    read_json_file()
    wf = WriteRowFile(file_path, num, get_one_json)
    wf.run()


@app_json.command()
def stream(bootstrap_servers: str = typer.Option(..., help="Kafka服务地址", show_default=False, rich_help_panel="参数"),
           topic: str = typer.Option(..., help="Kafka主题", show_default=False, rich_help_panel="参数"),
           delay: int = typer.Option(1, help="发送数据的间隔的秒数", rich_help_panel="参数")
           ):
    read_json_file()
    producer = StreamToKafka(bootstrap_servers, topic, delay, get_one_json)
    producer.run()


@app_json.command(help="向控制台每隔一段时间输出一条数据")
def test(delay: int = typer.Option(1, help="间隔秒数")):
    read_json_file()
    while True:
        print(get_one_json(), flush=True)
        time.sleep(delay)