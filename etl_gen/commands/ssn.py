import random
import time

import typer
import faker
from etl_gen.commands.tools import WriteRowFile, StreamToKafka

app_ssn = typer.Typer(
    name="ssn",
    help="生成大陆身份证号码，其中一半不合法"
)
f = faker.Faker("zh_cn")


def gen_id_card():
    return f.ssn()


def mix():
    result = random.choices("0123456789", k=18)
    return "".join(result)


def get_mixed_ssn():
    if random.random() > 0.5:
        return gen_id_card()
    else:
        return mix()


@app_ssn.command(help="创建一个文件，并向里面写入生成的数据")
def batch(
        num: int = typer.Option(..., help="文件中包含的数据行数", min=50, rich_help_panel="参数"),
        file_path: str = typer.Option(..., help="要生成的文件路径，此文件之前不能存在", rich_help_panel="参数", show_default=False)
):
    wf = WriteRowFile(file_path, num, get_mixed_ssn)
    wf.run()


@app_ssn.command(help="向Kafka持续生产数据")
def stream(bootstrap_servers: str = typer.Option(..., help="Kafka服务地址", show_default=False, rich_help_panel="参数"),
           topic: str = typer.Option(..., help="Kafka主题", show_default=False, rich_help_panel="参数"),
           delay: int = typer.Option(1, help="发送数据的间隔的秒数", rich_help_panel="参数")
           ):
    producer = StreamToKafka(bootstrap_servers, topic, delay, get_mixed_ssn())
    producer.run()


@app_ssn.command(help="向控制台每隔一段时间输出一条数据")
def test(delay: int = typer.Option(1, help="间隔秒数")):
    while True:
        print(get_mixed_ssn(), flush=True)
        time.sleep(delay)
