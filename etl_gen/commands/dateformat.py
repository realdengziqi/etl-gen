import random

import typer
import datetime
import time

from etl_gen.commands.tools import WriteRowFile, StreamToKafka

app_dateformat = typer.Typer(
    name="dateformat",
    help="生成各种常见格式的日期时间数据",
)

date_format_str = [
    ("20221231", "%Y%m%d"),
    ("GMT", "%a %b %d %Y %H:%M:%S GMT+0800"),
    ("2022-12-31", "%Y-%m-%d"),
    ("NYR_zh", "%Y年%m月%d日"),
    ("NYRSFM_zh", "%Y年%m月%d日 %H时%M分%S秒"),
    ("ISO8601", ""),
    ("CST", "%a %b %d %H:%M:%S CST %Y"),
]

# 可生成的起始日期
end_time = datetime.datetime.now()
start_time = datetime.datetime.now() + datetime.timedelta(days=-4000)

a1 = tuple(start_time.timetuple()[0:9])
a2 = tuple(end_time.timetuple()[0:9])

start = int(time.mktime(a1))
end = int(time.mktime(a2))


def gen_datetime():
    date_format = random.choice(date_format_str)
    t = random.randint(start, end)
    dt = datetime.datetime.fromtimestamp(t).strftime(date_format[1])
    if date_format[0] == "GMT":
        zone = random.randrange(-12, 13, 1)
        if zone:
            return dt.replace("+0800", "{:+03}00".format(zone))
        else:
            return dt.replace("+0800", "")

    if date_format[0] == "ISO8601":
        dt = datetime.datetime.fromtimestamp(t).astimezone().isoformat()
        zone = random.randrange(-12, 13, 1)
        if zone:
            return dt.replace("+08:00", "{:+03}:00".format(zone))
        else:
            return dt.replace("+08:00", "Z")
    return dt


@app_dateformat.command()
def batch(
        num: int = typer.Option(..., help="文件中包含的数据行数", min=50, rich_help_panel="参数"),
        file_path: str = typer.Option(..., help="要生成的文件路径，此文件之前不能存在", rich_help_panel="参数", show_default=False)
):
    wf = WriteRowFile(file_path, num, gen_datetime)
    wf.run()


@app_dateformat.command()
def stream(bootstrap_servers: str = typer.Option(..., help="Kafka服务地址", show_default=False, rich_help_panel="参数"),
           topic: str = typer.Option(..., help="Kafka主题", show_default=False, rich_help_panel="参数"),
           delay: int = typer.Option(1, help="发送数据的间隔的秒数", rich_help_panel="参数")
           ):
    producer = StreamToKafka(bootstrap_servers, topic, delay, gen_datetime)
    producer.run()


@app_dateformat.command(help="向控制台每隔一段时间输出一条数据")
def test(delay: int = typer.Option(1, help="间隔秒数")):
    while True:
        print(gen_datetime(), flush=True)
        time.sleep(delay)
