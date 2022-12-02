import time

import typer
import os
import random
from etl_gen.commands.tools import err_console, StreamToKafka

app_html = typer.Typer(
    name="html",
    help="""
    生成一堆包含电影信息的html数据，其中夹杂一些302状态码的异常数据
    """
)


def get_movie_html():
    current_path = os.path.dirname(__file__)
    html_list = os.listdir(os.path.join(current_path, "../resources/html_list"))
    r = random.choice(html_list)
    f = open(os.path.join(current_path, "../resources/html_list/", r), 'r', encoding="utf8")
    s = f.read()
    f.close()
    return s


@app_html.command(help="指定一个目录，并生成一批html文件。如果此目录不存在会自动创建目录。如果是已存在的目录则它必须为空目录")
def batch(num: int = typer.Option(100, help="要生成的html文件数量，不得少于100", min=100, rich_help_panel="参数"),
          path: str = typer.Option(..., help="生成文件的存放目录", rich_help_panel="参数", show_default=False)
          ):
    # 如果不存在这个路径就应该创建
    if os.path.exists(path):
        if not os.path.isdir(path):
            err_console.print(path + "路径不是目录")
            typer.Exit(1)
        if len(os.listdir(path)):
            err_console.print(path + "目录非空")
            typer.Exit(1)
    else:
        try:
            os.makedirs(path)
        except PermissionError as e:
            err_console.print("没有创建" + path + "的权限")
            typer.Exit(1)

    # 如果上面的检查都通过
    current_path = os.path.abspath(path)
    for i in range(1, num + 1):
        with open(os.path.join(current_path,str(i) + ".html"), "w", encoding="utf-8") as f:
            s = get_movie_html()
            f.write(s)


@app_html.command(help="向Kafka持续生产数据")
def stream(bootstrap_servers: str = typer.Option(..., help="Kafka服务地址", show_default=False, rich_help_panel="参数"),
           topic: str = typer.Option(..., help="Kafka主题", show_default=False, rich_help_panel="参数"),
           delay: int = typer.Option(1, help="发送数据的间隔的秒数", rich_help_panel="参数")
           ):
    producer = StreamToKafka(bootstrap_servers, topic, delay, get_movie_html)
    producer.run()


@app_html.command(help="向控制台每隔一段时间输出一条数据")
def test(delay: int = typer.Option(1, help="间隔秒数")):
    while True:
        print(get_movie_html(), flush=True)
        time.sleep(delay)
