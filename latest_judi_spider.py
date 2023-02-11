import datetime
import logging
import re
import threading
from urllib.parse import parse_qs,urlparse,urljoin
from pymongo import MongoClient
import scrapy
from unidecode import unidecode
from isodate import parse_datetime
from rich.console import Console
import requests
from concurrent.futures import ThreadPoolExecutor
import schedule
from traceback import print_exc
import time


class JudiSpider():
    con = Console()
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
    end_date = "V8432"
    # start_date = "V4900"
    start_date = "V8370"
    # start_date = "V6879"

    def __init__(self):
        self.collection = 'Judicial_Baja_California_Sur'
        self.MONGODB_HOST = '104.225.140.236'
        self.MONGODB_PORT = '27017'
        self.MONGODB_USER = 'Pad32'
        self.MONGODB_PASS = 'lGhg4S8AYZ85o7qe'
        self.mongo_url = 'mongodb://' + self.MONGODB_USER + ':' + self.MONGODB_PASS + '@' + self.MONGODB_HOST + ':' + self.MONGODB_PORT + '/Crudo'
        # self.mongo_url = "mongodb://localhost:27017"
        # self.mongo_db = 'testdb'
        self.mongo_db = 'Crudo'
        
        self.lock = threading.Lock()

    def send_request(self, url, get=None, post=None):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
        }
        if get:
            try:
                resp = requests.get(url, headers=headers, timeout=10)
            except:
                print('error in send_request get')
            else:
                return resp
        elif post:
            payload = post['data']
            try:
                resp = requests.post(url, data=payload, headers=headers, timeout=10)
            except:
                print('error in send_request post')
            else:
                return resp

    def initiate(self):
        url = 'https://e-tribunalbcs.mx/AccesoLibre/LiAcuerdos.aspx'
        return self.send_request(url, get=True)
    
    def collect_juzgados(self, response):
        sel = scrapy.Selector(text=response.text)
        for a in sel.xpath("(//a[@class='a' and contains(@href, 'LiAcuerdos')])[position() >3]"):
            entidad = a.xpath(".//ancestor::table/@id").get().lower().replace('tbl','')
            juzgado = a.xpath("./text()").get()
            materia = self.find_materia(juzgado)
            url = urljoin(response.url, a.xpath("./@href").get())
            juz_id = parse_qs(urlparse(url).query)['JuzId'][0]
            self.juzgados[url] = [juzgado, materia, entidad, juz_id]
        url = 'https://e-tribunalbcs.mx/AccesoLibre/LiAcuerdosBusqueda.aspx?MpioId=3&MpioDescrip=La%20Paz&JuzId=1&JuzDescrip=PRIMERO%20MERCANTIL&MateriaID=C&MateriaDescrip=Mercantil'
        return self.send_request(url, get=True)
    
    def back_to_past(self, response):
        self.temp_entidad = 'lapaz'
        sel = scrapy.Selector(text=response.text)
        payload = self.prepare_post(sel,entidad=self.temp_entidad, back=True)
        if not type(payload) == list:
            resp = self.send_request(url=response.url, post={'data':payload})
            return self.back_to_past(resp)
            # yield scrapy.FormRequest(url=response.url, formdata=payload, callback=self.back_to_past, dont_filter=True)
        # stop when start-date found
        else:
            # start-date included
            print(f" [+] start-date found! {payload[0]['__EVENTARGUMENT']}")
            # yield scrapy.FormRequest(url=response.url, formdata=payload[0], callback=self.parse_juzgado, dont_filter=True)
            resp = self.send_request(url=response.url, post={"data":payload[0]})
            return resp

    def parse_juzgado(self, response):
        sel = scrapy.Selector(text=response.text)
        year = sel.xpath("//table[@id='ctl00_ContentPlaceHolder1_Calendar1']//table/tr/td[position()=2]/text()").get()[-4:]
        # one month
        for day in sel.xpath("//table[@id='ctl00_ContentPlaceHolder1_Calendar1']/tr[position()>2]/td/a"):
            # if "26" in day.xpath("./@title").get():
            # 29 de noviembre 2022
            date_ = day.xpath("./@title").get().lower() +" "+ year
            fecha = self.create_fechas(date_)
                # if fecha == '2022/09/26':
                # lapaz+8039, lopaz+8040 etc
            day_args = []
            for url, juz_mat_ent_juzid in self.juzgados.items():
                # if "Primera Sala Unitaria en Materia Civil".lower() in juz_mat_ent_juzid[0].lower():
                entidad = juz_mat_ent_juzid[-2]
                juz_id = juz_mat_ent_juzid[-1]
                day_id = juz_id+entidad+re.search("(?:')([0-9].*)(?:')", day.xpath("./@href").get()).group(1)+juz_mat_ent_juzid[0][-10:]
                if not day_id in self.days_gone:
                    self.days_gone.append(day_id)
                    self.local_db.write(f"{day_id}\n")
                    payload = self.prepare_post(sel, day=day)
                    day_args.append((url, payload, juz_mat_ent_juzid, fecha, self.lock))
            with ThreadPoolExecutor(max_workers=30) as exec:
                exec.map(self.parse_day, day_args)
            print(f"\r [+] Fecha: {fecha}",end='')
            # yield responses
        # return None
        # end-date excluded
        # go to next month (default)
        payload = self.prepare_post(sel,entidad=self.temp_entidad)
        if payload:
            resp = self.send_request(url=response.url, post={"data":payload})
            self.parse_juzgado(resp)
        else:
            print(f" [+] end-date found")

    def parse_day(self, day_args):
        url, payload, juz_mat_ent_juzid, fecha, lock = day_args
        response = self.send_request(url, post={'data':payload})
        if not response:
            response = self.send_request(url, post={'data': payload})
        # count = 0
        # entidad
        entidad = juz_mat_ent_juzid[-2]
        sel = scrapy.Selector(text=response.text)
        # print(f'''{juz_mat_ent_juzid[0]}, {len(sel.xpath("//table[@id='ctl00_ContentPlaceHolder1_tblResultados']/tbody/tr/td[@valign][1]//text()").getall())}''')
        for row in sel.xpath("//table[@id='ctl00_ContentPlaceHolder1_tblResultados']/tbody/tr"):
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
                    tipo = partes[0].split('-')[0].replace('.','')
                    if len(tipo) <= 35:
                        pass

            # E = ACTOR
            actor = ''
            demando_part = ''
            partes = row.xpath("./td[@valign][2]//text()").getall()
            if partes:
                partes_list = partes
                partes = " ".join(partes).upper().replace('\n',' ')
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
            
            item = {
                "actor":self.clean(actor),
                "demandado":self.clean(demando),
                "entidad":self.clean(self.mappings[entidad]),
                "expediente":self.clean(expediente),
                "fecha":self.clean(fecha),
                "fuero":'COMUN',
                "juzgado":self.clean(juz_mat_ent_juzid[0]),
                "tipo":self.clean(tipo),
                "acuerdos":self.clean(acuerdos),
                "monto":'',
                "fecha_presentacion":'',
                "actos_reclamados":'',
                "actos_reclamados_especificos":'',
                "Naturaleza_procedimiento":'',
                "Prestación_demandada":'',
                "Organo_jurisdiccional_origen":self.clean(Organo_jurisdiccional_origen),
                "expediente_origen":expediente_origen,
                "materia":juz_mat_ent_juzid[1],
                "submateria":'',
                "fecha_sentencia":'',
                "sentido_sentencia":'',
                "resoluciones":'',
                "origen":"PODER JUDICIAL DEL ESTADO DE BAJA CALIFORNIA SUR",
                "fecha_insercion":'',
                "fecha_tecnica":'',
            }
            self.insert_db(item, lock)
    
    def clean(self, string):
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
        elif string == None:
            string = ''
        return string

    def prepare_post(self, sel, entidad=None, back=None, day=None):
        viewstate = sel.xpath("//input[@id='__VIEWSTATE']/@value").get()
        validation = sel.xpath("//input[@id='__EVENTVALIDATION']/@value").get()
        if day:
            day_id = re.search(r"(?:')([0-9].*)(?:')", day.xpath("./@href").get()).group(1)
        elif entidad:
            if back:
                previous_month_id = sel.xpath("(//table[@id='ctl00_ContentPlaceHolder1_Calendar1']//table//a)[1]/@href").get()
                day_id = re.search(r"(?:')(V[0-9].*)(?:')", previous_month_id).group(1)
                if self.start_date == day_id:
                    start_payload = {
                        "__EVENTTARGET":"ctl00$ContentPlaceHolder1$Calendar1",
                        "__EVENTARGUMENT":day_id,
                        "__VIEWSTATE":viewstate,
                        "__EVENTVALIDATION":validation,
                    }
                    return [start_payload]
            else:
                next_month_id = sel.xpath("(//table[@id='ctl00_ContentPlaceHolder1_Calendar1']//table//a)[2]/@href").get()
                day_id = re.search(r"(?:')(V[0-9].*)(?:')", next_month_id).group(1)
                if self.end_date == day_id:
                    return None
        payload = {
            "__EVENTTARGET":"ctl00$ContentPlaceHolder1$Calendar1",
            "__EVENTARGUMENT":day_id,
            "__VIEWSTATE":viewstate,
            "__EVENTVALIDATION":validation,
        }
        return payload
    
    def create_fechas(self, raw_fecha):
        day = re.search("([0-9]{1,2})(?:\sde)", raw_fecha).group(1)
        year = re.search("([0-9]{4})", raw_fecha).group(1)
        month = self.months[re.search("(de.*)(?:[0-9]{4})", raw_fecha).group(1).strip()]
        fecha = year+"/"+month.zfill(2)+"/"+day.zfill(2)
        return fecha

    def find_materia(self, link_text):
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

    def insert_db(self, item, lock):
        fecha = item.get("fecha")
        expediente = item.get("expediente")
        entidad = item.get("entidad")
        actor = item.get("actor")
        tipo = item.get("tipo")
        try:
            with lock:
                result = self.db[self.collection].count_documents({"fecha":fecha, "expediente":expediente, "entidad":entidad,'actor':actor,'tipo':tipo})
        # in case of issue, just pass
        except Exception:
            print_exc()
            return None
        if result:
            # print(" [+] Duplicate item ")
            pass
        else:
            fecha = item.get('fecha')
            today = datetime.datetime.now()
            fecha_insercion = parse_datetime(today.isoformat())
            fecha_tecnica = parse_datetime(datetime.datetime.strptime(fecha, "%Y/%m/%d").isoformat())
            item['fecha_insercion'] = fecha_insercion
            item['fecha_tecnica'] = fecha_tecnica
            try:
                with lock:
                    self.db[self.collection].insert_one(dict(item))
            except Exception:
                print_exc()
                return None


    def cal_start_end(self):
        url = 'https://e-tribunalbcs.mx/AccesoLibre/LiAcuerdosBusqueda.aspx?MpioId=3&MpioDescrip=La%20Paz&JuzId=1&JuzDescrip=PRIMERO%20MERCANTIL&MateriaID=C&MateriaDescrip=Mercantil'
        response = self.send_request(url, get=True)
        sel = scrapy.Selector(text=response.text)
        self.start_date = sel.xpath("(//table[@id='ctl00_ContentPlaceHolder1_Calendar1']//table//a)[1]/@href").get()
        self.start_date = re.search(r"(?:')(V[0-9].*)(?:')", self.start_date).group(1)
        self.end_date = sel.xpath("(//table[@id='ctl00_ContentPlaceHolder1_Calendar1']//table//a)[2]/@href").get()
        self.end_date = re.search(r"(?:')(V[0-9].*)(?:')", self.end_date).group(1)
        # update the self.start & self.end


    def main_func(self):
        try:
            self.client = MongoClient(self.mongo_url)
            self.db = self.client[self.mongo_db]
            self.local_db = open("localdb.txt",'a+')
            self.local_db.seek(0)
            self.days_gone = self.local_db.read().split('\n')
            self.cal_start_end()
            print(self.start_date)
            print(self.end_date)
            main_page = self.initiate()
            lapaz_calendar = self.collect_juzgados(response=main_page)
            start_page = self.back_to_past(response=lapaz_calendar)
            # daily
            # start_page = lapaz_calendar
            self.parse_juzgado(response=start_page)
        except (KeyboardInterrupt, Exception):
            self.con.print_exception()
        finally:
            self.client.close()
            self.local_db.close()
            

crawl = JudiSpider()
crawl.main_func()

schedule.every(48).hours.do(crawl.main_func)
while True:
    schedule.run_pending()
    scheduled_for = schedule.next_run()
    print(f"\r [+] Scheduled at {str(scheduled_for)}", end='')
    time.sleep(3600)


