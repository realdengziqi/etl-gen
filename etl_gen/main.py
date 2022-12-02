import typer
from etl_gen.commands.address import app_address
from etl_gen.commands.dateformat import app_dateformat
from etl_gen.commands.float import app_float
from etl_gen.commands.ip import app_ip
from etl_gen.commands.mixedhtml import app_html
from etl_gen.commands.mixedJson import app_json
from etl_gen.commands.mixedxml import app_xml
from etl_gen.commands.ssn import app_ssn


app = typer.Typer(
    name="etl-gen",
    help="""
    生成ETL练习数据
    """
)

app.add_typer(app_address)
app.add_typer(app_dateformat)
app.add_typer(app_float)
app.add_typer(app_ip)
app.add_typer(app_html)
app.add_typer(app_json)
app.add_typer(app_xml)
app.add_typer(app_ssn)

