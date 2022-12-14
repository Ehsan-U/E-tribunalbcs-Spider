import datetime
import re
from urllib.parse import parse_qs,urlparse
import scrapy
from unidecode import unidecode
from isodate import parse_datetime
from rich.console import Console
from scrapy.loader import ItemLoader
from ..items import JudicialItem
from scrapy import signals

class JudiSpider(scrapy.Spider):
    con = Console()
    name = 'judi_spider'
    allowed_domains = ['tribunalbcs.mx']
    juzgados = {}
    months = {
        "de enero":"01",
        "de febrero":"02",
        "de marzo":"03",
        "de abril":"04",
        "de mayo":"05",
        "de junio":"06",
        "de julio":"07",
        "de agosto":"08",
        "de septiembre":"09",
        "de octubre":"10",
        "de noviembre":"11",
        "de diciembre":"12"
    }
    mappings = {
        "lapaz":"LA PAZ",
        "loscabos":"LOS CABOS",
        "comondu":"COMONDU",
        "loreto":"LORETO",
        "mulege":"MULEGE",
    }
    daily = False
    goto_flag = True
    # start_dates = {
    #     "lapaz":"V4839",
    #     "loscabos":"V5387",
    #     "comondu":"V5357",
    #     "loreto":"V5418",
    #     "mulege":"V5418"
    # }
    # 2022/12
    start_dates = {
        "lapaz":"V8340",
        "loscabos":"V8340",
        "comondu":"V8340",
        "loreto":"V8340",
        "mulege":"V8340"
    }
    # 2019/12
    # start_dates = {
    #     "lapaz":"V7244",
    #     "loscabos":"V7244",
    #     "comondu":"V7244",
    #     "loreto":"V7244",
    #     "mulege":"V7244"
    # }
    # 2017/12
    # start_dates = {
    #     "lapaz":"V6514",
    #     "loscabos":"V6514",
    #     "comondu":"V6514",
    #     "loreto":"V6514",
    #     "mulege":"V6514"
    # }
    

    def start_requests(self):
        url = 'https://e-tribunalbcs.mx/AccesoLibre/LiAcuerdos.aspx'
        yield scrapy.Request(url, callback=self.collect_juzgados)

    # extract all links (juzgados) and decide correspoding materia
    def collect_juzgados(self, response):
        sel = scrapy.Selector(text=response.text)
        for a in sel.xpath("(//a[@class='a' and contains(@href, 'LiAcuerdos')])[position() >3]"):
            entidad = a.xpath(".//ancestor::table/@id").get().lower().replace('tbl','')
            juzgado = a.xpath("./text()").get()
            materia = self.find_materia(juzgado)
            url = response.urljoin(a.xpath("./@href").get())
            self.juzgados[url] = [juzgado, materia, entidad]

        # juz_mat_ent is a tuple ,e.g (juzgado, materia, entidad)
        for url, juz_mat_ent in self.juzgados.items():
            # if 'LABORAL' in juz_mat_ent[-2]:
            #     continue
            # else:
            yield scrapy.Request(url, callback=self.parse_juzgado, dont_filter=True, cb_kwargs={"juz_mat_ent":juz_mat_ent})
                

    def parse_juzgado(self, response, juz_mat_ent):
        sel = scrapy.Selector(text=response.text)
        year = sel.xpath("((//table[@id='ctl00_ContentPlaceHolder1_Calendar1']/tr)[1]//tr/td)[2]/text()").get()[-4:]
        entidad = juz_mat_ent[-1]
        juz_id = parse_qs(urlparse(response.url).query)['JuzId'][0]
        # one month
        for day in sel.xpath("//table[@id='ctl00_ContentPlaceHolder1_Calendar1']/tr/td/a"):
            # 29 de noviembre 2022
            date_ = day.xpath("./@title").get().lower() +" "+ year
            fecha = self.create_fechas(date_)
            # if fecha == '2017/12/05':
            # lapaz+8039, lopaz+8040 etc
            day_id = juz_id+entidad+re.search("(?:')([0-9].*)(?:')", day.xpath("./@href").get()).group(1)
            if not day_id in self.days_gone:
                self.days_gone.append(day_id)
                self.local_db.write(f"{day_id}\n")
                payload = self.prepare_post(sel, day=day)
                yield scrapy.FormRequest(url=response.url, formdata=payload, callback=self.parse_day, dont_filter=True, cb_kwargs={"juz_mat_ent":juz_mat_ent, "fecha":fecha})
                # break
        if bool(self.daily):
            # print('daily mode')
            pass
        else:
            # go to previous month
            payload = self.prepare_post(sel,entidad=entidad)
            if payload:
                yield scrapy.FormRequest(url=response.url, formdata=payload, callback=self.parse_juzgado, dont_filter=True, cb_kwargs={"juz_mat_ent":juz_mat_ent})
            else:
                pass

    def parse_day(self, response, juz_mat_ent, fecha):
        count = 0
        # entidad
        entidad = juz_mat_ent[-1]

        sel = scrapy.Selector(text=response.text)
        for row in sel.xpath("//table[@id='ctl00_ContentPlaceHolder1_tblResultados']/tbody/tr"):
            # C = expediente
            expediente = ''
            numero = row.xpath("./td[@valign][1]//text()").getall()
            if numero:
                expediente = " ".join(numero).upper()
                # if both expediente,amparo in expediente field then remove amparo
                if "EXPEDIENTE" in expediente:
                    if "AMPARO" in expediente:
                        expediente = re.search("(?:EXPEDIENTE)((.|\n)*)(?:AMPARO.*)", expediente).group(1)
                    else:
                        expediente = expediente.replace("EXPEDIENTE:", '')

            # D = TIPO
            tipo = ''
            partes = row.xpath("(./td[@valign])[2]//text()").getall() + row.xpath("(./td[@valign])[3]//text()").getall()
            joined_partes = " ".join(partes)
            if partes:
                if "DELITO:" in joined_partes.upper():
                    if re.search("(DELITO:.*?)(?:.-)", joined_partes):
                        tipo = re.search("(DELITO:.*?)(?:.-)", joined_partes).group(1)
                elif "EXPEDIENTILLO" in partes[0][:15].upper():
                    tipo = "EXPEDIENTILLO"
                elif "JUICIO EJECUTIVO MERCANTIL" in partes[0].upper():
                    tipo = "JUICIO EJECUTIVO MERCANTIL"
                elif "EJECUTIVO MERCANTIL" in partes[0].upper():
                    tipo = "EJECUTIVO MERCANTIL"
                elif "EXHORTO" in partes[0][:8].upper():
                    tipo = 'EXHORTO'
                elif ".-" in partes[0]:
                    tipo = partes[0].split('-')[0].replace('.','')
                    if len(tipo) <= 35:
                        pass

            # E = ACTOR
            actor = ''
            demando_part = ''
            partes = row.xpath("./td[@valign][2]//text()").getall()
            if partes:
                partes_list = partes
                partes = " ".join(partes).upper()
                if " VS" in partes:
                    orig_partes = partes
                    partes = partes.split(' VS')[0]
                    if "B.C.S." in partes or "BAJA CALIFORNIA SUR" in partes:
                        # startwith B.C.S. , BAJA CALIFORNIA SUR , - till VS
                        if re.search("(?:B.C.S.\s|BAJA\sCALIFORNIA\sSUR\s|-)(.*?)(?:VS)", orig_partes):
                            actor = re.search("(?:B.C.S.\s|BAJA\sCALIFORNIA\sSUR\s|-)(.*?)(?:VS)", orig_partes).group(1)
                    # startswith - till VS
                    elif "-" in partes:
                        if re.search("(?:[^0-9]-[^0-9])((.|\n)*?)(?:VS)", orig_partes):
                            actor = re.search("(?:[^0-9]-[^0-9])((.|\n)*?)(?:VS)", orig_partes).group(1)
                    # when only actor and demando exists in partes (0 index = actor)
                    else:
                        actor = partes
                elif "PROMOVIDO POR" in partes:
                    if re.search("(?:PROMOVIDO POR:|PROMOVIDO POR)((.|\n)*?)(?:ANTE\sEL|\()", partes):
                        if juz_mat_ent[1].upper() == 'PENAL':
                            demando_part = re.search("(?:PROMOVIDO POR:|PROMOVIDO POR)((.|\n)*?)(?:ANTE\sEL|\()", partes).group(1)
                        else:
                            actor = re.search("(?:PROMOVIDO POR:|PROMOVIDO POR)((.|\n)*?)(?:ANTE\sEL|\()", partes).group(1)
                    elif re.search("(?:PROMOVIDO POR:|PROMOVIDO POR)((.|\n)*?)(?:EN CONTRA DE|,)", partes):
                        actor = re.search("(?:PROMOVIDO POR:|PROMOVIDO POR)((.|\n)*?)(?:EN CONTRA DE|,)", partes).group(1)
                elif ".-" in partes and "," in partes and "(ACUERDO)" in partes:
                    if re.search("(?:.-)(.*)(?:\(ACUERDO\))", partes):
                        actor = re.search("(?:.-)(.*)(?:\(ACUERDO\))", partes).group(1)
                elif ".-" in partes:
                    actor = partes_list[0].split(".-")[-1]

            # F = DEMANDO
            demando = demando_part
            acuerdos_part = ''
            partes = row.xpath("./td[@valign][2]//text()").getall()
            if partes and not demando:
                partes = partes[0].upper()
                if " VS" in partes:
                    demando = partes.split(" VS")[-1]
                    if ".-" in demando:
                        acuerdos_part = demando.split('.-')[-1]
                        demando = demando.split('.-')[0]
                elif "EN CONTRA DE" in demando:
                    if len(partes.split("EN CONTRA DE")) == 2: 
                        demando = partes.split("EN CONTRA DE")[-1]

            # G = ACUERDOS
            acuerdos = acuerdos_part
            partes = row.xpath("(./td[@valign])[2]//text()").getall()
            sintesis = row.xpath("(./td[@valign])[3]//text()").getall()
            if partes:
                if actor and demando:
                    # take whole text
                    if "SENTENCIA" in partes[-1].upper():
                        acuerdos += " ".join(partes)
                    # take rest text after demando(F)
                    else:
                        if "(ACUERDO)" in partes[-1].upper():
                            acuerdos += partes[-1]
                    if sintesis:
                        acuerdos = acuerdos +" "+ " ".join(sintesis)
                # take whole text
                elif not actor and not demando:
                    acuerdos += " ".join(partes)
                    if sintesis:
                        acuerdos = acuerdos +" "+ " ".join(sintesis)
                # take rest text after demando(F)
                elif actor:
                    acuerdos += partes[-1]
                    if sintesis:
                        acuerdos = acuerdos +" "+ " ".join(sintesis)
                # take whole text
                else:
                    acuerdos += " ".join(partes)
                    if sintesis:
                        acuerdos = acuerdos +" "+ " ".join(sintesis)

            # H = Organo_jurisdiccional_origen
            Organo_jurisdiccional_origen = ''
            partes = row.xpath("(./td[@valign])[2]//text()").getall()
            if partes:
                Organo_jurisdiccional_origen = re.search("(?:PROCEDENTE\sDEL|REMITIDO\sPOR EL|\.-)((.|\n)*)(DEUCIDO\sDEL|DEDUCIDO\sDEL|B\.C\.S\.|DERIVADO\sDE|BAJA\sCALIFORNIA\sSUR)", partes[0])
                if Organo_jurisdiccional_origen:
                    Organo_jurisdiccional_origen = Organo_jurisdiccional_origen.group(1) + Organo_jurisdiccional_origen.group(3)
                
            # I = EXPEDIENTE ORIGEN 
            expediente_origen = ''
            partes = row.xpath("(./td[@valign])[2]//text()").getall()
            if partes:
                if "OFICIO NUMERO" in partes[0] or "JUICIO DE AMPARO" in partes[0] or "JUICIO DE AMPARO INDIRECTO" in partes[0] or "EXPEDIENTE " in partes[0]: 
                    if re.search("(?:OFICIO\sNUMERO\s)([0-9]+/[0-9]+)", partes[0]):
                        expediente_origen = re.search("(?:OFICIO\sNUMERO\s)([0-9]+/[0-9]+)", partes[0]).group(1)
                    elif re.search("(?:JUICIO\sDE\sAMPARO\sINDIRECTO\s)([0-9]+/[0-9]+)", partes[0]):
                        expediente_origen = re.search("(?:JUICIO\sDE\sAMPARO\sINDIRECTO\s)([0-9]+/[0-9]+)", partes[0]).group(1)
                    elif re.search("(?:EXPEDIENTE\s)([0-9]+/[0-9]+)", partes[0]):
                        expediente_origen = re.search("(?:EXPEDIENTE\s)([0-9]+/[0-9]+)", partes[0]).group(1)
                    elif re.search("(?:JUICIO\sDE\sAMPARO:\s)([0-9]+/[0-9]+)", partes[0]):
                        expediente_origen = re.search("(?:JUICIO\sDE\sAMPARO:\s)([0-9]+/[0-9]+)", partes[0]).group(1)
            
            loader = ItemLoader(item=JudicialItem())  
            loader.add_value('actor',value=actor)
            loader.add_value('demandado',value=demando)
            loader.add_value('entidad',value=self.mappings[entidad])
            loader.add_value('expediente',value=expediente)
            loader.add_value('fecha',value=fecha)
            loader.add_value('fuero',value='COMUN')
            loader.add_value('juzgado',value=juz_mat_ent[0])
            loader.add_value('tipo',value=tipo)
            loader.add_value('acuerdos',value=acuerdos)
            loader.add_value('monto',value='')
            loader.add_value('fecha_presentacion',value='')
            loader.add_value('actos_reclamados',value='')
            loader.add_value('actos_reclamados_especificos',value='')
            loader.add_value('Naturaleza_procedimiento',value='')
            loader.add_value('Prestación_demandada',value='')
            loader.add_value('Organo_jurisdiccional_origen',value=Organo_jurisdiccional_origen)
            loader.add_value('expediente_origen',value=expediente_origen)
            loader.add_value('materia',value=juz_mat_ent[1])
            loader.add_value('submateria',value='')
            loader.add_value('fecha_sentencia',value='')
            loader.add_value('sentido_sentencia',value='')
            loader.add_value('resoluciones',value='')
            loader.add_value('origen',value='PODER JUDICIAL DEL ESTADO DE BAJA CALIFORNIA SUR')
            loader.add_value('fecha_insercion',value='')
            loader.add_value('fecha_tecnica',value='')
            # if count < 3:
            #     count+=1
            yield loader.load_item()
            # else:
            #     # only one item
            #     break

    def prepare_post(self, sel, entidad=None,day=None):
        if day:
            day_id = re.search(r"(?:')([0-9].*)(?:')", day.xpath("./@href").get()).group(1)
        elif entidad:
            previous_month_id = sel.xpath("(//table[@id='ctl00_ContentPlaceHolder1_Calendar1']//table//a)[1]/@href").get()
            day_id = re.search(r"(?:')(V[0-9].*)(?:')", previous_month_id).group(1)
            if self.start_dates[entidad.lower()] == day_id:
                return None
        viewstate = sel.xpath("//input[@id='__VIEWSTATE']/@value").get()
        validation = sel.xpath("//input[@id='__EVENTVALIDATION']/@value").get()
        payload = {
            "__EVENTTARGET":"ctl00$ContentPlaceHolder1$Calendar1",
            "__EVENTARGUMENT":day_id,
            "__VIEWSTATE":viewstate,
            "__EVENTVALIDATION":validation,
        }
        return payload
                
    def find_materia(self, link_text):
        link_text = link_text.lower()
        laboral = ['laboral']
        civil = ['mercantil', 'civil', 'materia civil', 'civil y familiar']
        familiar = ['familiar', 'consignaciones']
        penal = ['penal', 'adolescentes', 'sanciones']
        admin = ['materia administrativa']
        laboral = [l for l in laboral if l in link_text]
        civil = [c for c in civil if c in link_text]
        familiar = [f for f in familiar if f in link_text]
        penal = [p for p in penal if p in link_text]
        admin = [ad for ad in admin if ad in link_text]
        if laboral:
            materia = 'laboral'
        elif civil:
            materia = 'civil'
        elif familiar:
            materia = 'familiar'
        elif penal:
            materia = 'penal'
        elif admin:
            materia = 'administrativa'
        return materia.upper()        

    def create_fechas(self, raw_fecha):
        day = re.search("([0-9]{1,2})(?:\sde)", raw_fecha).group(1)
        year = re.search("([0-9]{4})", raw_fecha).group(1)
        month = self.months[re.search("(de.*)(?:[0-9]{4})", raw_fecha).group(1).strip()]
        fecha = year+"/"+month.zfill(2)+"/"+day.zfill(2)
        return fecha
    
    def closed(self, reason):
        self.local_db.close()

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(JudiSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        return spider

    def spider_opened(self, spider):
        self.local_db = open("localdb.txt",'a+')
        self.local_db.seek(0)
        self.days_gone = self.local_db.read().split('\n')
        

