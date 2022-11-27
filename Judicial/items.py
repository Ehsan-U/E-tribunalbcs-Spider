# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from itemloaders.processors import TakeFirst,MapCompose,Join
from unidecode import unidecode

def clean(string):
    string = string.strip()
    new_string = ''
    for char in string:
        if char.upper() == 'Ñ':
            new_string += char.upper()
        else:
            new_string += unidecode(char).upper()
    return new_string.replace('"',"'")

class JudicialItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    actor = scrapy.Field(
        input_processor = MapCompose(clean),
        output_processor = Join()
    )
    demandado = scrapy.Field(
        input_processor = MapCompose(clean),
        output_processor = Join()
    )
    entidad = scrapy.Field(
        input_processor = MapCompose(clean),
        output_processor = Join()
    )
    expediente = scrapy.Field(
        input_processor = MapCompose(clean),
        output_processor = Join()
    )
    fecha = scrapy.Field(
        input_processor = MapCompose(clean),
        output_processor = Join()
    )
    fuero = scrapy.Field(
        output_processor = Join()
    )
    juzgado = scrapy.Field(
        input_processor = MapCompose(clean),
        output_processor = Join()
    )
    tipo = scrapy.Field(
        input_processor = MapCompose(clean),
        output_processor = Join()
    )
    acuerdos = scrapy.Field(
        input_processor = MapCompose(clean),
        output_processor = Join()
    )
    monto = scrapy.Field(
        output_processor = Join()
    )
    fecha_presentacion = scrapy.Field(
        output_processor = Join()
    )
    actos_reclamados = scrapy.Field(
        output_processor = Join()
    )
    actos_reclamados_especificos = scrapy.Field(
        output_processor = Join()
    )
    Naturaleza_procedimiento = scrapy.Field(
        output_processor = Join()
    )
    Prestación_demandada = scrapy.Field(
        output_processor = Join()
    )
    Organo_jurisdiccional_origen = scrapy.Field(
        input_processor = MapCompose(clean),
        output_processor = Join()
    )
    expediente_origen = scrapy.Field(
        output_processor = Join()
    )
    materia = scrapy.Field(
        output_processor = Join()
    )
    submateria = scrapy.Field(
        output_processor = Join()
    )
    fecha_sentencia = scrapy.Field(
        output_processor = Join()
    )
    sentido_sentencia = scrapy.Field(
        output_processor = Join()
    )
    resoluciones = scrapy.Field(
        output_processor = Join()
    )
    origen = scrapy.Field(
        output_processor = Join()
    )
    fecha_insercion = scrapy.Field(
        output_processor = TakeFirst()
    )
    fecha_tecnica = scrapy.Field(
        output_processor = TakeFirst()
    )
