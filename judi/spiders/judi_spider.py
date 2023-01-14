import re
from urllib.parse import parse_qs, urlparse, urljoin
import scrapy
from scrapy import signals
from unidecode import unidecode

class JudiScraper(scrapy.Spider):
    name = 'judi_spider'
    flag = True
    allowed_domains = ['e-tribunalbcs.mx']
    # start_date = "V4900"
    start_date = "V8370"
    months = {
        "de enero": "01",
        "de febrero": "02",
        "de marzo": "03",
        "de abril": "04",
        "de mayo": "05",
        "de junio": "06",
        "de julio": "07",
        "de agosto": "08",
        "de septiembre": "09",
        "de octubre": "10",
        "de noviembre": "11",
        "de diciembre": "12"
    }
    mappings = {
        "lapaz": "LA PAZ",
        "loscabos": "LOS CABOS",
        "comondu": "COMONDU",
        "loreto": "LORETO",
        "mulege": "MULEGE",
    }
    juzgados = {}
    test_Day = 0


    def start_requests(self):
        url = 'https://e-tribunalbcs.mx/AccesoLibre/LiAcuerdosBusqueda.aspx?MpioId=3&MpioDescrip=La%20Paz&JuzId=1&JuzDescrip=PRIMERO%20MERCANTIL&MateriaID=C&MateriaDescrip=Mercantil'
        yield scrapy.Request(url, callback=self.time_machine)


    def time_machine(self, response):
        print(response.xpath("(//table[@id='ctl00_ContentPlaceHolder1_Calendar1']//table//a)[1]/@href").get())
        month_id = self.calender_id(response, backward=True)
        if month_id != self.start_date:
            formdata = self.extract_form(response, month_id)
            yield scrapy.FormRequest(url=response.url, formdata=formdata, dont_filter=True, callback=self.time_machine)
        # reached start_date
        else:
            url = 'https://e-tribunalbcs.mx/AccesoLibre/LiAcuerdos.aspx'
            yield scrapy.Request(url, callback=self.extract_juzgados, cb_kwargs={"start_response":response})


    def extract_juzgados(self, response, start_response):
        sel = scrapy.Selector(text=response.text)
        for a in sel.xpath("(//a[@class='a' and contains(@href, 'LiAcuerdos')])[position() >3]"):
            entidad = a.xpath(".//ancestor::table/@id").get().lower().replace('tbl', '')
            juzgado = a.xpath("./text()").get()
            materia = self.find_materia(juzgado)
            url = urljoin(response.url, a.xpath("./@href").get())
            juz_id = parse_qs(urlparse(url).query)['JuzId'][0]
            self.juzgados[url] = (juzgado, materia, entidad, juz_id)
        # init_scrape
        month_id = self.calender_id(start_response, backward=True)
        formdata = self.extract_form(start_response, month_id)
        yield scrapy.FormRequest(url=start_response.url, formdata=formdata, dont_filter=True, callback=self.parse)


    def parse(self, response):
        year = response.xpath("//table[@id='ctl00_ContentPlaceHolder1_Calendar1']//table/tr/td[position()=2]/text()").get()[-4:]
        # self.priority = 100
        for day in response.xpath("//table[@id='ctl00_ContentPlaceHolder1_Calendar1']/tr[position()>2]/td/a"):
            date_ = day.xpath("./@title").get().lower() + " " + year
            fecha = self.create_fechas(date_)
            for url, juz_mat_ent_juzid in self.juzgados.items():
                entidad = juz_mat_ent_juzid[-2]
                juz_id = juz_mat_ent_juzid[-1]
                day_uuid = juz_id+entidad+re.search("(?:')([0-9].*)(?:')", day.xpath("./@href").get()).group(1)+juz_mat_ent_juzid[0][-10:]
                if not day_uuid in self.days_gone:
                    self.days_gone.append(day_uuid)
                    self.local_db.write(f"{day_uuid}\n")
                    day_id = self.calender_id(None, day=day)
                    formdata = self.extract_form(response, day_id)
                    yield scrapy.FormRequest(url, formdata=formdata, callback=self.parse_day, dont_filter=True, cb_kwargs={"juz_mat_ent_juzid":juz_mat_ent_juzid, "fecha":fecha})
            # self.logger.info(f" [+] Fecha: {fecha}")
            # print(f" [+] Fecha: {fecha}")
            # self.priority -= 1



    def parse_day(self, response, juz_mat_ent_juzid, fecha):
        entidad = juz_mat_ent_juzid[-2]
        for row in response.xpath("//table[@id='ctl00_ContentPlaceHolder1_tblResultados']/tbody/tr"):
            # C = expediente
            expediente = ''
            numero = row.xpath("./td[@valign][1]//text()").getall()
            if numero:
                expediente = " ".join(numero).upper().replace('\n', ' ')
                # if both expediente,amparo in expediente field then remove amparo
                if "EXPEDIENTE" in expediente:
                    if "AMPARO" in expediente:
                        expediente = re.search("(?:EXPEDIENTE)((.|\n)*)(?:AMPARO.*)", expediente).group(1)
                    else:
                        expediente = expediente.replace("EXPEDIENTE:", '')

            # D = TIPO
            tipo = ''
            partes = row.xpath("(./td[@valign])[2]//text()").getall() + row.xpath("(./td[@valign])[3]//text()").getall()
            joined_partes = " ".join(partes).replace('\n', ' ')
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
                    tipo = partes[0].split('-')[0].replace('.', '')
                    if len(tipo) <= 35:
                        pass

            # E = ACTOR
            actor = ''
            demando_part = ''
            partes = row.xpath("./td[@valign][2]//text()").getall()
            if partes:
                partes_list = partes
                partes = " ".join(partes).upper().replace('\n', ' ')
                if " VS" in partes:
                    orig_partes = partes
                    partes = partes.split(' VS')[0]
                    if "B.C.S." in partes or "BAJA CALIFORNIA SUR" in partes:
                        # startwith B.C.S. , BAJA CALIFORNIA SUR , - till VS
                        if re.search("(?:B.C.S.\s|BAJA\sCALIFORNIA\sSUR\s|-)(.*?)(?:VS)", orig_partes):
                            actor = re.search("(?:B.C.S.\s|BAJA\sCALIFORNIA\sSUR\s|-)(.*?)(?:VS)", orig_partes).group(1)
                        if not actor:
                            if re.search("(.*?)(?:VS)", orig_partes):
                                actor = re.search("(.*?)(?:VS)", orig_partes).group(1)
                    # startswith - till VS
                    elif "-" in partes:
                        if re.search("(?:[^0-9]-[^0-9])((.|\n)*?)(?:VS)", orig_partes):
                            actor = re.search("(?:[^0-9]-[^0-9])((.|\n)*?)(?:VS)", orig_partes).group(1)
                    # when only actor and demando exists in partes (0 index = actor)
                    else:
                        actor = partes
                elif "PROMOVIDO POR" in partes:
                    if re.search("(?:PROMOVIDO POR:|PROMOVIDO POR)((.|\n)*?)(?:ANTE\sEL|\()", partes):
                        if juz_mat_ent_juzid[1].upper() == 'PENAL':
                            demando_part = re.search("(?:PROMOVIDO POR:|PROMOVIDO POR)((.|\n)*?)(?:ANTE\sEL|\()",
                                                     partes).group(1)
                        else:
                            actor = re.search("(?:PROMOVIDO POR:|PROMOVIDO POR)((.|\n)*?)(?:ANTE\sEL|\()",
                                              partes).group(1)
                    elif re.search("(?:PROMOVIDO POR:|PROMOVIDO POR)((.|\n)*?)(?:EN CONTRA DE|,)", partes):
                        actor = re.search("(?:PROMOVIDO POR:|PROMOVIDO POR)((.|\n)*?)(?:EN CONTRA DE|,)", partes).group(
                            1)
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
                partes = partes[0].upper().replace('\n', ' ')
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
                        acuerdos = acuerdos + " " + " ".join(sintesis)
                # take whole text
                elif not actor and not demando:
                    acuerdos += " ".join(partes)
                    if sintesis:
                        acuerdos = acuerdos + " " + " ".join(sintesis)
                # take rest text after demando(F)
                elif actor:
                    acuerdos += partes[-1]
                    if sintesis:
                        acuerdos = acuerdos + " " + " ".join(sintesis)
                # take whole text
                else:
                    acuerdos += " ".join(partes)
                    if sintesis:
                        acuerdos = acuerdos + " " + " ".join(sintesis)

            # H = Organo_jurisdiccional_origen
            Organo_jurisdiccional_origen = ''
            partes = row.xpath("(./td[@valign])[2]//text()").getall()
            if partes:
                Organo_jurisdiccional_origen = re.search(
                    "(?:PROCEDENTE\sDEL|REMITIDO\sPOR EL|\.-)((.|\n)*)(DEUCIDO\sDEL|DEDUCIDO\sDEL|B\.C\.S\.|DERIVADO\sDE|BAJA\sCALIFORNIA\sSUR)",
                    partes[0])
                if Organo_jurisdiccional_origen:
                    Organo_jurisdiccional_origen = Organo_jurisdiccional_origen.group(
                        1) + Organo_jurisdiccional_origen.group(3)

            # I = EXPEDIENTE ORIGEN
            expediente_origen = ''
            partes = row.xpath("(./td[@valign])[2]//text()").getall()
            if partes:
                if "OFICIO NUMERO" in partes[0] or "JUICIO DE AMPARO" in partes[0] or "JUICIO DE AMPARO INDIRECTO" in \
                        partes[0] or "EXPEDIENTE " in partes[0]:
                    if re.search("(?:OFICIO\sNUMERO\s)([0-9]+/[0-9]+)", partes[0]):
                        expediente_origen = re.search("(?:OFICIO\sNUMERO\s)([0-9]+/[0-9]+)", partes[0]).group(1)
                    elif re.search("(?:JUICIO\sDE\sAMPARO\sINDIRECTO\s)([0-9]+/[0-9]+)", partes[0]):
                        expediente_origen = re.search("(?:JUICIO\sDE\sAMPARO\sINDIRECTO\s)([0-9]+/[0-9]+)",
                                                      partes[0]).group(1)
                    elif re.search("(?:EXPEDIENTE\s)([0-9]+/[0-9]+)", partes[0]):
                        expediente_origen = re.search("(?:EXPEDIENTE\s)([0-9]+/[0-9]+)", partes[0]).group(1)
                    elif re.search("(?:JUICIO\sDE\sAMPARO:\s)([0-9]+/[0-9]+)", partes[0]):
                        expediente_origen = re.search("(?:JUICIO\sDE\sAMPARO:\s)([0-9]+/[0-9]+)", partes[0]).group(1)

            item = {
                "actor": self.clean(actor),
                "demandado": self.clean(demando),
                "entidad": self.clean(self.mappings[entidad]),
                "expediente": self.clean(expediente),
                "fecha": self.clean(fecha),
                "fuero": 'COMUN',
                "juzgado": self.clean(juz_mat_ent_juzid[0]),
                "tipo": self.clean(tipo),
                "acuerdos": self.clean(acuerdos),
                "monto": '',
                "fecha_presentacion": '',
                "actos_reclamados": '',
                "actos_reclamados_especificos": '',
                "Naturaleza_procedimiento": '',
                "Prestación_demandada": '',
                "Organo_jurisdiccional_origen": self.clean(Organo_jurisdiccional_origen),
                "expediente_origen": expediente_origen,
                "materia": juz_mat_ent_juzid[1],
                "submateria": '',
                "fecha_sentencia": '',
                "sentido_sentencia": '',
                "resoluciones": '',
                "origen": "PODER JUDICIAL DEL ESTADO DE BAJA CALIFORNIA SUR",
                "fecha_insercion": '',
                "fecha_tecnica": '',
            }
            print(f" [+] {fecha}")
            yield item

    def extract_form(self, response, id):
        viewstate = response.xpath("//input[@id='__VIEWSTATE']/@value").get()
        validation = response.xpath("//input[@id='__EVENTVALIDATION']/@value").get()
        formdata = {
            "__EVENTTARGET": "ctl00$ContentPlaceHolder1$Calendar1",
            # "__EVENTARGUMENT": ,
            "__EVENTARGUMENT": id,
            "__VIEWSTATE": viewstate,
            "__EVENTVALIDATION": validation,
        }
        return formdata


    def create_fechas(self, raw_fecha):
        day = re.search("([0-9]{1,2})(?:\sde)", raw_fecha).group(1)
        year = re.search("([0-9]{4})", raw_fecha).group(1)
        month = self.months[re.search("(de.*)(?:[0-9]{4})", raw_fecha).group(1).strip()]
        fecha = year + "/" + month.zfill(2) + "/" + day.zfill(2)
        return fecha


    @staticmethod
    def find_materia(link_text):
        link_text = link_text.lower()
        if link_text == "Tercera Sala Unitaria Civil y de Justicia Administrativa (Materia Administrativa)".lower():
            materia = 'administrativa'
            return materia.upper()
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


    @staticmethod
    def calender_id(response, backward=None, day=None):
        if day:
            day_id = re.search(r"(?:')([0-9].*)(?:')", day.xpath("./@href").get()).group(1)
            return day_id
        elif backward:
            month_id = response.xpath("(//table[@id='ctl00_ContentPlaceHolder1_Calendar1']//table//a)[1]/@href").get()
        else:
            month_id = response.xpath("(//table[@id='ctl00_ContentPlaceHolder1_Calendar1']//table//a)[2]/@href").get()
        calender_id = re.search(r"(?:')(V[0-9].*)(?:')", month_id).group(1)
        return calender_id.strip()

    @staticmethod
    def clean(string):
        if string:
            #  if first char is . remove it
            if string.startswith('.'):
                string = string.replace('.', '', 1)
            string = re.sub("([*]{1,5})", '', re.sub("([*]{5}\sY\s[*]{5})", '', re.sub("(\*.*@.*\.[a-zA-Z]{2,4})", "",re.sub("\s\s+", " ", string))))
            string = string.replace("*****,", '').replace("*****", '')
            string = string.strip()
            new_string = ''
            for char in string:
                if char.upper() == 'Ñ':
                    new_string += char.upper()
                else:
                    new_string += unidecode(char).upper()
            if new_string.endswith(','):
                new_string = new_string.replace(',', '')
            return new_string.replace('"', "'").replace("\n", ' ')
        elif string == None:
            string = ''
        return string

##################################################################

    def closed(self, reason):
        self.local_db.close()

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(JudiScraper, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        return spider

    def spider_opened(self, spider):
        self.local_db = open("localdb.txt", 'a+')
        self.local_db.seek(0)
        self.days_gone = self.local_db.read().split('\n')
