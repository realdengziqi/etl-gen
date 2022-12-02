import random
import time

import typer
from etl_gen.commands.tools import StreamToKafka, WriteRowFile
import os

help_text = """
生成中国境内的随机ipv4地址
示例：103.30.228.163
"""

app_ip = typer.Typer(
    name="ip",
    help=help_text,

)

ips = []


def read_file():
    current_path = os.path.dirname(__file__)
    with open(os.path.join(current_path, "../resources/ip_table")) as f:
        data = f.readlines()
        l = [j.strip().split("\t") for j in data]

        for i in l[1:]:
            t = (ip2num(i[0]), ip2num(i[1]))
            ips.append(t)


def ip2num(ip):
    ip = [int(x) for x in ip.split('.')]
    return ip[0] << 24 | ip[1] << 16 | ip[2] << 8 | ip[3]


def num2ip(num):
    return '%s.%s.%s.%s' % (
        (num & 0xff000000) >> 24,
        (num & 0x00ff0000) >> 16,
        (num & 0x0000ff00) >> 8,
        num & 0x000000ff
    )


def gen_ips(start, end):
    """生成IP地址"""
    until = True
    ip = ''
    while until:
        ip = num2ip(random.randint(start, end))
        if not ip.endswith(".0"):
            until = False
    return ip


def get_one_ip():
    ran = random.choice(ips)
    return gen_ips(ran[0], ran[1])





@app_ip.command(help="向Kafka持续生产数据")
def batch(
        num: int = typer.Option(..., help="文件中包含的数据行数", min=100, rich_help_panel="参数"),
        file_path: str = typer.Option(..., help="要生成的文件路径，此文件之前不能存在", rich_help_panel="参数", show_default=False)
):
    read_file()
    wf = WriteRowFile(file_path, num, get_one_ip)
    wf.run()


@app_ip.command(help="向Kafka持续生产数据")
def stream(bootstrap_servers: str = typer.Option(..., help="Kafka服务地址", show_default=False, rich_help_panel="参数"),
           topic: str = typer.Option(..., help="Kafka主题", show_default=False, rich_help_panel="参数"),
           delay: int = typer.Option(1, help="发送数据的间隔的秒数", rich_help_panel="参数")
           ):
    read_file()
    producer = StreamToKafka(bootstrap_servers, topic, delay, get_one_ip)
    producer.run()


@app_ip.command(help="向控制台每隔一段时间输出一条数据")
def test(delay: int = typer.Option(1, help="间隔秒数")):
    read_file()
    while True:
        print(get_one_ip(), flush=True)
        time.sleep(delay)