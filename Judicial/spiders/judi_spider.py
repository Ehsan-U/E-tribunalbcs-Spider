import datetime
import re
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
    start_dates = {
        "lapaz":"V4839",
        "loscabos":"V5387",
        "comondu":"V5357",
        "loreto":"V5418",
        "mulege":"V5418"
    }
    
    def start_requests(self):
        url = 'https://e-tribunalbcs.mx/AccesoLibre/LiAcuerdos.aspx'
        yield scrapy.Request(url, callback=self.collect_juzgados)

    # extract all links (juzgados) and decide correspoding materia
    def collect_juzgados(self, response):
        sel = scrapy.Selector(text=response.text)
        for a in sel.xpath("(//a[@class='a' and contains(@href, 'LiAcuerdos')])[position() >3]"):
            # (//a[@class='a' and contains(@href, 'LiAcuerdos')])[position() >3]
            entidad = a.xpath(".//ancestor::table/@id").get().lower().replace('tbl','')
            juzgado = a.xpath("./text()").get()
            materia = self.find_materia(juzgado)
            url = response.urljoin(a.xpath("./@href").get())
            self.juzgados[url] = [juzgado, materia, entidad]

        # juz_mat_ent is a tuple ,e.g (juzgado, materia, entidad)
        for url, juz_mat_ent in self.juzgados.items():
            yield scrapy.Request(url, callback=self.parse_juzgado, dont_filter=True, cb_kwargs={"juz_mat_ent":juz_mat_ent})
            break    

    def parse_juzgado(self, response, juz_mat_ent):
        sel = scrapy.Selector(text=response.text)
        year = sel.xpath("((//table[@id='ctl00_ContentPlaceHolder1_Calendar1']/tr)[1]//tr/td)[2]/text()").get()[-4:]
        entidad = juz_mat_ent[-1]
        # one month
        for day in sel.xpath("//table[@id='ctl00_ContentPlaceHolder1_Calendar1']/tr/td/a"):
            date_ = day.xpath("./@title").get().lower() +" "+ year
            # lapaz+8039, lopaz+8040 etc
            day_id = entidad+re.search("(?:')([0-9].*)(?:')", day.xpath("./@href").get()).group(1)
            if not day_id in self.days_gone:
                print(date_)
                self.local_db.write(f"{day_id}\n")
                payload = self.prepare_post(sel, day=day)
                yield scrapy.FormRequest(url=response.url, formdata=payload, callback=self.parse_day, dont_filter=True, cb_kwargs={"juz_mat_ent":juz_mat_ent, "fecha":date_})
        # go to previous month
        payload = self.prepare_post(sel,entidad=entidad)
        if payload:
            yield scrapy.FormRequest(url=response.url, formdata=payload, callback=self.parse_juzgado, dont_filter=True, cb_kwargs={"juz_mat_ent":juz_mat_ent})
        else:
            print(f"\n[+] Ends on {entidad}")

    def parse_day(self, response, juz_mat_ent, fecha):
        fecha, fecha_insercion, fecha_tecnica = self.create_fechas(fecha)
        # entidad
        entidad = juz_mat_ent[-1]

        sel = scrapy.Selector(text=response.text)
        for row in sel.xpath("//table[@id='ctl00_ContentPlaceHolder1_tblResultados']/tbody/tr"):
            # C = expediente
            numero = row.xpath("./td[@valign][1]//text()").getall()
            if numero:
                expediente = " ".join(numero).upper()
                # if both expediente,amparo in expediente field then remove amparo
                if "EXPEDIENTE" in expediente:
                    if "AMPARO" in expediente:
                        expediente = re.search("(?:EXPEDIENTE)(.*)(?:AMPARO.*)", expediente).group(1)
                    else:
                        expediente = expediente.replace("EXPEDIENTE:", '')
                else:
                    pass   
            else:
                expediente = ''

            # D = TIPO
            partes = row.xpath("./td[@valign][2]//text()").getall()
            if partes:
                if "EXPEDIENTILLO" in partes[0][:15].upper():
                    tipo = "EXPEDIENTILLO"
                elif "JUICIO EJECUTIVO MERCANTIL" in partes[0].upper():
                    tipo = "JUICIO EJECUTIVO MERCANTIL"
                elif "EXHORTO" in partes[0][:8].upper():
                    tipo = 'EXHORTO'
                elif "-" in partes[0]:
                    tipo = partes[0].split('-')[0].replace('.','')
                    if len(tipo) <= 35:
                        pass
                    else:
                        tipo = ''
                else:
                    tipo = ''
            else:
                tipo = ''

            # E = ACTOR
            partes = row.xpath("./td[@valign][2]//text()").getall()
            if partes:
                actor = " ".join(partes).upper()
                if " VS" in actor:
                    raw_actor = actor
                    actor = actor.split(' VS')[0]
                    if "-" in actor:
                        actor = re.search("(?:[^0-9]-[^0-9])(.*?)(?:VS)", raw_actor).group(1)
                    else:
                        pass
                elif "PROMOVIDO POR" in actor:
                    try:
                        actor = re.search("(?:PROMOVIDO POR)(.*?)(?:EN CONTRA DE|,)", actor).group(1)
                    except AttributeError:
                        actor = ''
                else:
                    actor = ''
            else:
                actor = ''

            # F = DEMANDO
            partes = row.xpath("./td[@valign][2]//text()").getall()
            if partes:
                demando = partes[0].upper()
                if " VS" in demando:
                    demando = demando.split(" VS")[-1]
                elif "EN CONTRA DE" in demando:
                    demando = demando.split("EN CONTRA DE")[-1]
                else:
                    demando = ''
            else:
                demando = ''

            # G = ACUERDOS
            partes = row.xpath("(./td[@valign])[2]//text()").getall()
            sintesis = row.xpath("(./td[@valign])[3]//text()").getall()
            if partes:
                if actor and demando:
                    # take whole text
                    if "SENTENCIA" in partes[-1].upper():
                        acuerdos = " ".join(partes)
                    # take rest text after demando(F)
                    else:
                        acuerdos = partes[-1]
                    if sintesis:
                        acuerdos = acuerdos +" "+ " ".join(sintesis)
                # take whole text
                elif not actor and not demando:
                    acuerdos = " ".join(partes)
                    if sintesis:
                        acuerdos = acuerdos +" "+ " ".join(sintesis)
                # take rest text after demando(F)
                elif actor:
                    acuerdos = partes[-1]
                    if sintesis:
                        acuerdos = acuerdos +" "+ " ".join(sintesis)
                # take whole text
                else:
                    acuerdos = " ".join(partes)
                    if sintesis:
                        acuerdos = acuerdos +" "+ " ".join(sintesis)
            else:
                acuerdos = ''

            # H = Organo_jurisdiccional_origen
            partes = row.xpath("(./td[@valign])[2]//text()").getall()
            if partes:
                Organo_jurisdiccional_origen = re.search("(?:PROCEDENTE DEL|REMITIDO POR EL)(.*)(?:DEUCIDO DEL|DEDUCIDO DEL|DERIVADO DE)", partes[0])
                if Organo_jurisdiccional_origen:
                    Organo_jurisdiccional_origen = Organo_jurisdiccional_origen.group(1)
                else:
                    Organo_jurisdiccional_origen = ''
            else:
                Organo_jurisdiccional_origen = ''
                
            # I = EXPEDIENTE ORIGEN 
            partes = row.xpath("(./td[@valign])[2]//text()").getall()
            if partes:
                expediente_origen = re.findall("([0-9]+/[0-9]+)", partes[0])
                if expediente_origen:
                    expediente_origen = ", ".join(expediente_origen)
                else:
                    expediente_origen = ''

            loader = ItemLoader(item=JudicialItem())  
            loader.add_value('actor',value=actor)
            loader.add_value('demandado',value=demando)
            loader.add_value('entidad',value=entidad)
            loader.add_value('expediente',value=expediente)
            loader.add_value('fecha',value=fecha)
            loader.add_value('fuero',value='')
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
            loader.add_value('fecha_insercion',value=fecha_insercion)
            loader.add_value('fecha_tecnica',value=fecha_tecnica)
            yield loader.load_item()

    def prepare_post(self, sel, entidad=None,day=None):
        if day:
            day_id = re.search(r"(?:')([0-9].*)(?:')", day.xpath("./@href").get()).group(1)
        else:
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
        fecha = year+"/"+month+"/"+day
        today = datetime.datetime.now()
        fecha_insercion = parse_datetime(today.isoformat())
        fecha_tecnica = parse_datetime(datetime.datetime.strptime(fecha, "%Y/%m/%d").isoformat())
        return (fecha, fecha_insercion, fecha_tecnica)
    
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
        

