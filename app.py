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

#VARS
client_id = "9bec1ccd55d871163ac79dd5698295e8376e768d"
client_secret = "40b19e91450d1abfa404c41a22059d8695847331526b530c8fc6e9623468"
client_encoding = f"{client_id}:{client_secret}"
host = "https://www.bling.com.br"
state = "".join(random.choices(string.ascii_uppercase + string.digits, k=15))
session_tokens = ()
#ENCODING
client_encoding_bytes = client_encoding.encode()
b64 = base64.b64encode(client_encoding_bytes)
b64_str = b64.decode()

#função para o usuário autorizar o aplicativo e obter o token de autorização
def first_auth():
    cn_str = f"{host}/Api/v3/oauth/authorize?response_type=code&client_id={client_id}&state={state}"
    r = requests.get(cn_str)
    webbrowser.open(r.url)
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
    cn_str = f"{host}/Api/v3/oauth/token"
    headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Accept": "1.0",
    "Authorization": f"Basic {b64_str}"
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
    return (parsed["access_token"], parsed["refresh_token"])

#função para solicitar outro token de acesso
def refresh(refresh_token):
    global session_tokens
    cn_str = f"{host}/Api/v3/oauth/token"
    headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Accept": "1.0",
    "Authorization": f"Basic {b64_str}"
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

        r = requests.get(f"{host}/Api/v3/produtos", params=payload, headers=headers)
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

def api_estoque_post(db_response, error=False):
    #Criar aqui um método para atualizar o estoque do bling com base nas informações obtidas do sistema da Car Brasil
    print(f"(api_estoque_post) Fazendo POST em {len(db_response)} registros.")
    json_data = []
    with open("bling_product.json", "r+") as file:
            file_data = json.load(file)
            json_data.append(file_data)
    for prod in db_response:
        print(prod["codigo"], prod["descricao"])
        headers = {
            "Authorization": f"Bearer {session_tokens[0]}"
        }
        payload = {
            "produto":{
                "id": prod["product_id"]
            },
            "deposito":{
                "id": 3471220462
            },
            "operacao": "B",
            "preco": prod["custo"],
            "custo": prod["custo"],
            "quantidade": prod["estoque"],
            "observacoes": "API CARBRASIL-BLING POST",
            "data": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        r = requests.post(f"{host}/Api/v3/estoques", headers=headers, json=payload)
        parsed = json.loads(r.text)
        iter_data = {"codigo":prod["codigo"],"idestoque":parsed["data"]["id"]}
        json_data[0]["products"].append(iter_data)
        
    os.remove("bling_product.json")
    with open("bling_product.json", "w+") as file:
        json.dump(json_data[0], file, indent=4)
    print("(api_estoque_post) POST requests finalizadas com sucesso.")

def api_estoque_put(db_response):
    
    codigos_com_erro = []
    for product in db_response:
        headers = {
        "Authorization": f"Bearer {session_tokens[0]}"
        }
        payload = {
            "operacao": "B",
            "preco": product["custo"],
            "custo": product["custo"],
            "quantidade": product["estoque"],
            "observacoes": "API CARBRASIL-BLING PUT",
            "data": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        with open("bling_product.json") as file:
            file_data = json.load(file)

        for data in file_data["products"]:
            if data["codigo"] == product["codigo"]:
                r = requests.put(f"{host}/Api/v3/estoques/{data['idestoque']}", headers=headers, json=payload)
                if r.status_code == 404:
                    codigos_com_erro.append(product)
    ###
    ###PAREI AQUI (08/07/24) TERMINAR ESSA SEÇÃO
    ###
    if codigos_com_erro:
        print(f"\n(api_estoque_put) Foi encontrado um erro na atualização do registro dos seguintes produtos: \n{codigos_com_erro}\n \
              (api_estoque_put) Corrigindo o erro..")
    #    api_estoque_post(codigos_com_erro)
    print("(api_estoque_put) PUT requests finalizadas com sucesso. ")

def sync_routine():
    global session_tokens
    auth_code = first_auth()
    session_tokens = second_auth(auth_code)
    #usando threading para fazer o programa esperar 5 horas enquanto faz outras coisas
    auth_routine_threading = threading.Thread(target=auth_routine)
    auth_routine_threading.start()
    time.sleep(3)
    while True:
        os.system("cls")
        print(session_tokens)
        print("(sync_routine) Inicializando rotina de sincronização..")
        products = api_calls_get()
        db_response = database_get(products)
        has_idestoque = []
        not_idestoque = []
        print("(sync_routine) Analisando dados obtidos do banco de dados..")
        for x in db_response:
            with open("bling_product.json") as file:
                file_data = json.load(file)
                if not file_data["products"]:
                    not_idestoque.append(x)
                    continue
            for data in file_data["products"]:
                if data["codigo"] == x["codigo"]:
                    has_idestoque.append(x)
            if x not in has_idestoque:
                not_idestoque.append(x)
        print("(sync_routine) Análise completa.")
        if has_idestoque:
            api_estoque_put(has_idestoque)
        if not_idestoque:
            api_estoque_post(not_idestoque)
        print("(sync_routine) Rotina de sincronização finalizada. 10 segundos para a próxima sincronização")
        time.sleep(10)

sync_routine()




#Falta fazer a função de sincronizar o estoque e de rotina de atualização. Utilizar os mesmos conceitos aplicados de threading e de scheduling para aplicar uma sincronia
# a cada x tempo.
#preciso definir também o padrão de funcionamento do programa, eu não posso simplesmente clonar o estoque da CarBrasil no bling a cada x horas, eu tenho que fazer 
# uma requisição dos produtos do bling e pedir para o estoque da CarBrasil somente os produtos que vieram nessa requisição.
# Motivo: na data de hoje (06/07/24) estamos trabalhando apenas com o estoque que está contado, e não o estoque completo.


