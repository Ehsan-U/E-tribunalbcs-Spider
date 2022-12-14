# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import re
import scrapy
from itemloaders.processors import TakeFirst,MapCompose,Join
from unidecode import unidecode

def clean(string):
    if string:
        #  if first char is . remove it
        if string.startswith('.'):
            string = string.replace('.','', 1)
        string = re.sub("([*]{1,5})", '', re.sub("([*]{5}\sY\s[*]{5})", '', re.sub("(\*.*@.*\.[a-zA-Z]{2,4})", "", re.sub("\s\s+" , " ", string))))
        string = string.replace("*****,",'').replace("*****",'')
        string = string.strip()
        new_string = ''
        for char in string:
            if char.upper() == 'Ñ':
                new_string += char.upper()
            else:
                new_string += unidecode(char).upper()
        if new_string.endswith(','):
            new_string = new_string.replace(',', '')
        return new_string.replace('"',"'").replace("\n",' ')
    return string

def check(value):
    print(type(value))

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

