import random
import time

from etl_gen.commands.tools import err_console, StreamToKafka, WriteRowFile
import os
import typer

mixed_seed = []

app_float = typer.Typer(
    name="float",
    help="生成一堆夹杂汉字的浮点数数据"
)


def gen_float():
    return random.random() * random.randint(1, 100000)


def mixed(num):
    zi = random.randint(0x4e00, 0x9fbf)
    num = list(str(num))
    a = random.randrange(0, len(num))
    num.insert(a, chr(zi))
    return "".join(num)


def get_one_num():
    return mixed(gen_float())


@app_float.command(help="创建一个文件，并向里面写入生成的数据")
def batch(
        num: int = typer.Option(..., help="文件中包含的数据行数", min=1, rich_help_panel="参数"),
        file_path: str = typer.Option(..., help="要生成的文件路径，此文件之前不能存在", rich_help_panel="参数", show_default=False)
):
    wf = WriteRowFile(file_path, num, get_one_num)
    wf.run()


@app_float.command(help="向Kafka持续生产数据")
def stream(bootstrap_servers: str = typer.Option(..., help="Kafka服务地址", show_default=False, rich_help_panel="参数"),
           topic: str = typer.Option(..., help="Kafka主题", show_default=False, rich_help_panel="参数"),
           delay: int = typer.Option(1, help="发送数据的间隔的秒数", rich_help_panel="参数")
           ):
    producer = StreamToKafka(bootstrap_servers, topic, delay, get_one_num)
    producer.run()

@app_float.command(help="向控制台每隔一段时间输出一条数据")
def test(delay: int = typer.Option(1, help="间隔秒数")):
    while True:
        print(get_one_num(), flush=True)
        time.sleep(delay)
