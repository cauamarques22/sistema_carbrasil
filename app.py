import requests
import webbrowser
import base64
import random
import string
import time
import os
import json
import threading
import sched
import datetime
import pyodbc
import asyncio
import aiohttp
import itertools

#VARS
CLIENT_ID = "9bec1ccd55d871163ac79dd5698295e8376e768d"
CLIENT_SECRET = "40b19e91450d1abfa404c41a22059d8695847331526b530c8fc6e9623468"
CLIENT_ENCODING = f"{CLIENT_ID}:{CLIENT_SECRET}"
HOST = "https://www.bling.com.br"
state = "".join(random.choices(string.ascii_uppercase + string.digits, k=15))
session_tokens = ()
#idestoque_json = "bling_product_sample.json"
IDESTOQUE_JSON = r"C:\Users\supervisor\Desktop\bling_idestoque.json"


#ENCODING
CLIENT_ENCODING_BYTES = CLIENT_ENCODING.encode()
B64 = base64.b64encode(CLIENT_ENCODING_BYTES)
B64_STR = B64.decode()

#função para o usuário autorizar o aplicativo e obter o token de autorização
def first_auth():
    payload = {
        "client_id": CLIENT_ID,
        "state": state,
        "response_type": "code"
    }
    cn_str = f"{HOST}/Api/v3/oauth/authorize"
    r = requests.get(cn_str, params=payload)
    webbrowser.open(r.url)
    time.sleep(1)
    r2 = requests.get("https://cauamarques.pythonanywhere.com/")

    os.system("cls")
    print("Aguardando a Autorização do Usuário.")
    while len(r2.text) == 0:
        time.sleep(8)
        r2 = requests.get("https://cauamarques.pythonanywhere.com/")
    else:
        os.system("cls")
        print("Código de acesso Obtido.")
        print(r2.text)
        requests.get("https://cauamarques.pythonanywhere.com/clear")
    
    auth_code = json.loads(r2.text)
    print("(first_auth) Autenticação de primeiro estágio realizada.")
    return auth_code["code"]

#função para solicitar o primeiro token de acesso
def second_auth(auth_code):
    global session_tokens
    cn_str = f"{HOST}/Api/v3/oauth/token"
    headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Accept": "1.0",
    "Authorization": f"Basic {B64_STR}"
    }

    payload = {
    "grant_type":"authorization_code",
    "code":f"{auth_code}"
    }

    r = requests.post(cn_str, headers=headers, data=payload)
    parsed = json.loads(r.text)
    if "error" in parsed.keys():
        raise RuntimeError("API RETORNOU COM ERRO (second_auth)")
    
    print("(second_auth) Autenticação de segundo estágio realizada.")
    session_tokens = (parsed["access_token"], parsed["refresh_token"])

#função para solicitar outro token de acesso
def refresh(refresh_token):
    global session_tokens
    cn_str = f"{HOST}/Api/v3/oauth/token"
    headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Accept": "1.0",
    "Authorization": f"Basic {B64_STR}"
    }

    payload = {
    "grant_type":"refresh_token",
    "refresh_token":f"{refresh_token}"
    }

    r = requests.post(cn_str, headers=headers,data=payload)
    parsed = json.loads(r.text)
    session_tokens = (parsed["access_token"], parsed["refresh_token"])
    print("(refresh) Tokens Refreshed: ", session_tokens)

#esperando 5 horas para solicitar o outro token de acesso
def auth_routine():
    while True:
        scheduler = sched.scheduler(time.time, time.sleep)
        t = datetime.datetime.now() + datetime.timedelta(hours=5)
        scheduler.enterabs(t.timestamp() , 1, refresh, argument=(session_tokens[1],))
        print("(auth_routine) Rotina de autenticação agendada.")
        scheduler.run()
       
def api_calls_get():
    counter = 1
    products_per_page = []
    all_products = []
    print("(api_calls_get) Solicitando produtos ao Bling..")
    while True:
        headers = {
            "Authorization": f"Bearer {session_tokens[0]}"
        }

        payload = {
            "limite": 500,
            "pagina": counter,
            "criterio": 2
        }

        r = requests.get(f"{HOST}/Api/v3/produtos", params=payload, headers=headers)
        parsed = json.loads(r.text)

        if not parsed["data"]:
            break

        products_per_page.append(parsed["data"])
        counter +=1
    
    for x in products_per_page:
        for prod in x:
            all_products.append(prod)
    print("(api_calls_get) Produtos recebidos com sucesso.")
    return all_products

def database_get(products):
    cnxn_str = ("Driver={SQL SERVER};"
            "Server=CARBRASIL-HOST\\SQLEXPRESS;"
            "Database=bdados1;"
            "UID=sa;"
            "PWD=Tec12345678;")
    
    cnxn = pyodbc.connect(cnxn_str)
    cursor = cnxn.cursor()

    db_responses = []
    for product in products:
        response = cursor.execute(f"SELECT codprod, descricao, eatu, pvenda, custo_base FROM v_produtos1 WHERE ativa=0 AND codprod={int(product["codigo"])}")
        for x in response:
            document = {
                "product_id": product["id"],
                "codigo": x[0],
                "descricao": x[1].strip(),
                "estoque": float(x[2]),
                "preco_venda": float(x[3]),
                "custo": float(x[4])
            }
            db_responses.append(document)
    return db_responses

def salvar_json(json_data, error=False):
    if error:
        with open(IDESTOQUE_JSON, "r+") as file:
            file_data = json.load(file)
            file.seek(0)
            file.truncate()
            for product in json_data:
                for data in file_data["products"]:
                    if product["codigo"] == data["codigo"]:
                        index = file_data["products"].index(data)
                        file_data["products"][index]["idestoque"] = product["idestoque"]
            json.dump(file_data, file, indent=4)
        return

    with open(IDESTOQUE_JSON, "r+") as file:
        file_data = json.load(file)
        file.seek(0)
        file.truncate()
        for data in json_data:
            file_data["products"].append(data)
        json.dump(file_data, file, indent=4)

async def api_estoque_post(session, product):
    print(product["codigo"], product["descricao"])
    headers = {
        "Authorization": f"Bearer {session_tokens[0]}"
    }
    payload = {
        "produto":{
            "id": product["product_id"]
        },
        "deposito":{
            "id": 3471220462
        },
        "operacao": "B",
        "preco": product["custo"],
        "custo": product["custo"],
        "quantidade": product["estoque"],
        "observacoes": "API CARBRASIL-BLING POST",
        "data": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    async with session.request(method="POST",url=f"{HOST}/Api/v3/estoques", headers=headers, json=payload) as resp:
        response = await resp.json()
        prep_response = {"codigo":product["codigo"], "idestoque": response["data"]["id"]}
        return prep_response

async def async_post(verified_db_response, error=False):
    if error:
        print("(async_post: errors) Começando POST REQUESTS")
    else:
        print("(async_post) Começando POST REQUESTS")
    batched_responses = []
    batched_products = list(itertools.batched(verified_db_response, 3))
    async with aiohttp.ClientSession() as session:
        for product in batched_products:
            await asyncio.sleep(1.1)
            tasks = [asyncio.ensure_future(api_estoque_post(session, p)) for p in product]
            response = await asyncio.gather(*tasks)
            batched_responses.append(response)

    flattened_responses = list(itertools.chain.from_iterable(batched_responses))
    if error:
        salvar_json(flattened_responses, error=True)
        print("(async_post: errors) POST REQUESTS finalizadas com sucesso.")
        return True
    salvar_json(flattened_responses)
    print("(async_post) POST REQUESTS finalizadas com sucesso.")

async def api_estoque_put(session, product):
    print("Começando request: ", product["codigo"])
    headers = {
    "Authorization": f"Bearer {session_tokens[0]}"
    }
    payload = {
        "operacao": "B",
        "preco": product["custo"],
        "custo": product["custo"],
        "quantidade": product["estoque"],
        "observacoes": "API CARBRASIL-BLING PUT",
        "data": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    async with session.request(method="PUT", url=f"{HOST}/Api/v3/estoques/{product['id_estoque']}",  json=payload, headers=headers) as resp:
        read_resp =  await resp.content.read()
        if len(read_resp.decode()) > 0:
            prep_response = {"read_content":read_resp, "product_info":product}
            return prep_response
        return resp.status

async def async_put(verified_db_response):
    print(f"(async put) Começando PUT REQUESTS em {len(verified_db_response)} registros")
    error_not_found = []
    unknown_errors = []
    #Faz com que a lista de dicionários esteja separada por Tuplas de 3 dicionários 
    #verified_db_response: [{}, {} ...]
    #batched_products: [({},{},{}), ({},{},{}), ...]
    batched_products = list(itertools.batched(verified_db_response, 3))
    async with aiohttp.ClientSession() as session:
        for product in batched_products:
            await asyncio.sleep(1.3)
            tasks = [asyncio.ensure_future(api_estoque_put(session,p)) for p in product]
            responses = await asyncio.gather(*tasks)
            for x in responses:
                #Se a response não for o 204 (comportamento esperado), irá tentar converter a 
                # response em JSON e dar append caso seja um erro conhecido "RESOURCE NOT FOUND".
                if x != 204:
                    byte_response = x["read_content"]
                    reading = byte_response.decode()
                    try:
                        reading = json.loads(reading)
                        if "error" in reading.keys():
                            if reading["error"]["type"] == "RESOURCE_NOT_FOUND":
                                error_not_found.append(x["product_info"])
                            else: 
                                unknown_errors.append(x["product_info"])
                    #Houve casos onde esse erro foi lançado. A princípio será lidado como Unknown Error e ignorado.
                    except json.JSONDecodeError:
                        unknown_errors.append(x)
    #Caso a lista composta de erros tipo RESOURCE NOT FOUND tenha itens, ela chamará a função async post para lidar com esses erros
    if error_not_found:
        print("\n(async put) Os seguintes produtos retornarm com errros:\n",error_not_found)
        print("(async put) Tentando corrigir os erros")
        r = await asyncio.gather(async_post(error_not_found, error=True))
        if r:
            print("(async put) Erros corrigidos com sucesso.")
        else:
            print("(async put) Não foi possível corrigir os erros.")
        return
    #Se existem Unknown Erros, ele irá imprimir na tela.
    if unknown_errors:
        print("\n(async put) Foram encontrados erros não reconhecidos: \n", unknown_errors)
    print("(api_estoque_put) PUT requests finalizadas com sucesso. ")

def verify_db_response(db_response):
    print("(verify_db_response) Analisando dados obtidos do banco de dados..")
    has_idestoque = []
    not_idestoque = []
    codigos_ignorados = []
    #Abre/cria o arquivo ignore_codes.json
    try:
        with open("ignore_codes.json", "r+") as file:
            file_data = json.load(file)
            ignore_codes = file_data["codes"]
    except FileNotFoundError:
        with open("ignore_codes.json", "x") as file:
            file_data = {"codes":[]}
            json.dump(file_data, file, indent=4)

    for x in db_response:
        #Verifica se o x["codigo"] está nos ignore_codes
        if x["codigo"] in ignore_codes:
            codigos_ignorados.append(x["codigo"])
            continue
        #Verifica se o x["codigo"] tem idEstoque cadastrado no arquivo bling_idestqoue.json() e faz a distinção nos arquivos que tem, e que não tem.
        #Se esse arquivo não existir, ele cria um, e sai do loop.
        try:
            with open(IDESTOQUE_JSON) as file:
                file_data = json.load(file)
                if not file_data["products"]:
                    not_idestoque = db_response.copy()
                    break
            for data in file_data["products"]:
                if data["codigo"] == x["codigo"]:
                    x["id_estoque"] = data["idestoque"]
                    has_idestoque.append(x)
            if x not in has_idestoque:
                not_idestoque.append(x)
        except FileNotFoundError:
            with open(IDESTOQUE_JSON, "x") as file:
                json.dump({"products":[]}, file, indent=4)
                not_idestoque = db_response.copy()
                break
    print("(verify_db_response) IGNORE_CODES:", ignore_codes)
    print("(verify_db_response) CODIGOS IGNORADOS:", codigos_ignorados)
    print("(verify_db_response) Análise completa.")
    return (has_idestoque, not_idestoque)

def sync_routine():
    global session_tokens
    auth_code = first_auth()
    second_auth(auth_code)
    #usando threading para fazer o programa esperar 5 horas enquanto faz outras coisas
    auth_routine_threading = threading.Thread(target=auth_routine)
    auth_routine_threading.start()
    while True:
        os.system("cls")
        print(session_tokens)
        print("(sync_routine) Inicializando rotina de sincronização..")
        products = api_calls_get()
        db_response = database_get(products)
        has_idestoque, not_idestoque = verify_db_response(db_response)
        
        #Calling the Appropriate Function
        now = time.time()
        if has_idestoque:
            asyncio.run(async_put(has_idestoque))
        if not_idestoque:
            asyncio.run(async_post(not_idestoque))
        os.system("cls")
        after = time.time()
        time_spent = after-now
        print(f"O programa levou {time_spent:.2f} segundos para completar o ciclo de sincronização.")

sync_routine()