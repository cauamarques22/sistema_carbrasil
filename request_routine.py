#Dependencies
import aiohttp.client_exceptions
import datetime
import aiohttp
import asyncio
import itertools
import logging

#App modules
import auth_routine
import connect_database
import data_exchanger

logging.basicConfig(level=logging.DEBUG, filemode="a", filename="app_logs.log", format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("request_routine_module")

class ApiFunctions():
    def __init__ (self):
        self._pause_trigger = data_exchanger.PAUSE_EVENT
        self._stop_trigger = data_exchanger.STOP_EVENT
        self.UI = data_exchanger.UI

        self.semaphore = asyncio.Semaphore(1)
        
    def displayer(self, msg):
        print(msg)
        time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        self.UI.modulo1_textbox.insert('end', f"{time} - {msg}\n")

    async def create_stock_slave(self, session, product):
        self.displayer(f"(create_stock_slave) Começando request: {product['codigo_carbrasil']}")
        headers = {
            "Authorization": f"Bearer {auth_routine.AuthRoutine.session_tokens[0]}"
        }
        payload = {
            "produto":{
                "id": product["id_bling"]
            },
            "deposito":{
                "id": 3471220462
            },
            "operacao": "B",
            "preco": product["divergencias"]["preco"],
            "custo": product["divergencias"]["custo"],
            "quantidade": product["divergencias"]["estoque"],
            "observacoes": "API CARBRASIL-BLING POST",
            "data": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        try:
            async with session.request(method="POST",url=f"{auth_routine.AuthRoutine.HOST}estoques", headers=headers, json=payload) as resp:
                
                if resp.status == 504: #não pode ter decode, pois irá retornar um HTML e não um JSON
                    product["resposta_api"] = {"response_status": resp.status}
                    logger.error(f"Produto com erro: \n{product}")
                    return product
                elif resp.status != 201:
                    product["resposta_api"] = {"response_status": resp.status, "decoded_resp": await resp.json()}
                    logger.error(f"Produto com erro: \n{product}")
                    return product
                

                response = await resp.json()
                product["resposta_api"] = {"response_status": resp.status, "idEstoque": response["data"]["id"]}
                return product
        
        except aiohttp.client_exceptions.ClientOSError as err:
            product["resposta_api"] = {"response_status":"OSERROR"}
            logger.error(f"(create_stock_slave) WINERROR: {err.winerror}")
            logger.error("(create_stock_slave) PRODUCT THAT RAISED AN ERROR:")
            logger.error(f"(create_stock_slave) {product}")
            logger.exception("ClientOSError")
            return product
        
        except Exception as err:
            product["resposta_api"] = {"response_status":"CRITICAL_EXCEPTION"}
            logger.critical("(create_stock_slave) UNHANDLED EXCEPTION")
            logger.exception("(create_stock_slave) UNKNOWN EXCEPTION")
            logger.debug(f"Produto utilizado durante a exceção:\n{product}")
            raise err

    async def create_stock_main(self, instructions, error=False):
        self.displayer(f"{'(create_stock_main: errors)' if error else '(create_stock_main)'} Começando POST REQUESTS de {len(instructions)} produtos.")
        logger.info(f"{'(create_stock_main: errors)' if error else '(create_stock_main)'} Começando POST REQUESTS de {len(instructions)} produtos.")
        unknown_errors: list[dict] = []
        os_error: list[dict] = []
        critical_errors: list[dict] = []
        ok_status: list[dict] = []
        error_status: list[dict] = [] 

        batched_products = list(itertools.batched(instructions, 3))
        async with aiohttp.ClientSession() as session:
            for product in batched_products:
                await asyncio.sleep(1.1)
                tasks = [asyncio.create_task(self.create_stock_slave(session, p)) for p in product]
                response = await asyncio.gather(*tasks)
                for resp in response:

                    #Error Handling
                    if "decoded_resp" in resp["resposta_api"].keys():
                        if "error" in resp["resposta_api"]["decoded_resp"].keys():
                            unknown_errors.append(resp)
                    
                    elif resp["resposta_api"]["response_status"] == "OSERROR":
                        os_error.append(resp)
                    elif resp["resposta_api"]["response_status"] == 201:
                        ok_status.append(resp)
                    elif resp["resposta_api"]["response_status"] != 201:
                        unknown_errors.append(resp)
        
        #Atualizando campo do idEstoque no Banco de Dados
        conn = connect_database.conn_pool.get_connection()
        with conn:
            with conn.cursor() as cursor:
                for prod in ok_status:
                    cursor.execute(f"UPDATE db_sistema_intermediador SET idEstoque={prod["resposta_api"]['idEstoque']} WHERE codigo_carbrasil={prod['codigo_carbrasil']}")
            conn.commit()

        msg = f"(create_stock_main) POST REQUESTS FINALIZADAS, RESULTADOS: \nok_status:{len(ok_status)}\nunknown_errors:{len(unknown_errors)}\nos_errors: {len(os_error)}\nok_status: {len(ok_status)}"
        logger.info(msg)
        self.displayer(msg)

        if unknown_errors or os_error or critical_errors:
            error_status.extend(unknown_errors)
            error_status.extend(os_error)
            error_status.extend(critical_errors)
            logger.warn(f"Os seguintes produtos retornaram com erros:\n{error_status}")
            return (ok_status, error_status)
        
        return (ok_status, [])

    async def update_stock_slave(self, session:aiohttp.ClientSession, product: dict) -> dict:

        self.displayer(f"(update_stock_slave) Começando request: {product['codigo_carbrasil']}")
        headers = {
        "Authorization": f"Bearer {auth_routine.AuthRoutine.session_tokens[0]}"
        }
        payload = {
            "operacao": "B",
            "preco": product["divergencias"]["preco"],
            "custo": product["divergencias"]["custo"],
            "quantidade": product["divergencias"]["estoque"],
            "observacoes": "API CARBRASIL-BLING PUT",
            "data": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        try:
            async with session.request(method="PUT", url=f"{auth_routine.AuthRoutine.HOST}estoques/{product['idEstoque']}",  json=payload, headers=headers) as resp:
                resp_status = resp.status

                if resp_status == 504:
                    product["resposta_api"] = {"response_status": resp_status}
                    logger.error(f"Produto com erro: \n{product}")
                    return product

                elif resp_status  != 204:
                    product["resposta_api"] = {"decoded_resp": await resp.json(), "response_status": resp_status}
                    logger.error(f"Produto com erro: \n{product}")
                    return product

                product["resposta_api"] = {"response_status":resp_status}
                return product

        except aiohttp.client_exceptions.ClientOSError as err:
            
            product["resposta_api"] = {"response_status": "OSERROR"}
            logger.error(f"(api_estoque_put) WINERROR: {err.winerror}")
            logger.error("(api_estoque_put) PRODUCT THAT RAISED AN ERROR:")
            logger.error(f"(api_estoque_put): \n{product}")
            logger.exception("ClientOSError")
            return product
        
        except Exception as err:
            
            product["resposta_api"] = {"response_status":"CRITICAL_EXCEPTION"}
            logger.critical("(update_stock_slave) UNHANDLED EXCEPTION")
            logger.exception("(update_stock_slave) UNKNOWN EXCEPTION")
            logger.error(f"(update_stock_slave): \n{product}")
            raise err

    async def update_stock_main(self, instructions):
        self.displayer(f"(update_stock_main) Começando PUT REQUESTS em {len(instructions)} registros")
        error_not_found: list[dict] = []
        unknown_errors: list[dict] = []
        os_error: list[dict] = []
        ok_status: list[dict] = []
        error_status: list[dict] = []

        batched_products = list(itertools.batched(instructions, 3))
        async with aiohttp.ClientSession() as session:
            for product in batched_products:
                await asyncio.sleep(1.1)
                tasks = [asyncio.create_task(self.update_stock_slave(session,p)) for p in product]
                responses = await asyncio.gather(*tasks)
               
                #Error Handling
                for resp in responses:
                    
                    if "decoded_resp" in resp["resposta_api"].keys():
                        if "error" in resp["resposta_api"]["decoded_resp"].keys():
                            if resp["resposta_api"]["decoded_resp"]["error"]["type"] == "RESOURCE_NOT_FOUND":
                                error_not_found.append(resp)
                                continue
                            unknown_errors.append(resp)
                    
                    elif resp["resposta_api"]["response_status"] == "OSERROR":
                        os_error.append(resp)
                    elif resp["resposta_api"]["response_status"] == 204:
                        ok_status.append(resp)
                    elif resp["resposta_api"]["response_status"] == 504:
                        unknown_errors.append(resp)
        
        msg = f"(update_stock_main) PUT REQUESTS FINALIZADAS, RESULTADOS:\nok_status:{len(ok_status)}\nerror_not_found:{len(error_not_found)}\nunknown_errors:{len(unknown_errors)}\nos_errors: {len(os_error)}\n"
        self.displayer(msg)
        logger.info(msg)

        #Correção de error_not_found
        if error_not_found:
            r = await asyncio.create_task(self.create_stock_main(error_not_found, error=True))
            if r[0]:
                if not r[1]:
                    self.displayer("(update_stock_main) Erros corrigidos")
                    logger.info("(update_stock_main) Erros corrigidos")
                else:
                    self.displayer("(update_stock_main) Alguns errors corrigidos, porém outros produtos retornaram com erros")
                    logger.warn("(update_stock_main) Alguns errors corrigidos, porém outros produtos retornaram com erros")
                    logger.warn(f"(update_stock_main) Retorno de status OK da função create_stock_main: \n{r[0]}")
                    logger.warn(f"(update_stock_main) Retorno de status ERRO da função create_stock_main: \n{r[1]}")
            else:
                self.displayer("(update_stock_main) ERROS NÃO FORAM CORRIGIDOS")
                logger.error("(update_stock_main) ERROS NÃO FORAM CORRIGIDOS")
        
        if error_not_found or unknown_errors or os_error:
            error_status.extend(error_not_found)
            error_status.extend(unknown_errors)
            error_status.extend(os_error)
            logger.warn(f"Os seguintes produtos retornaram com erros:\n{error_status}")
            return (ok_status, error_status)

        return(ok_status, [])

    async def update_product_slave(self, session: aiohttp.ClientSession, product: dict): 
        
        self.displayer(f"(update_product_slave) Atualizando Código: {product['codigo_carbrasil']}")
        headers = {
        "Authorization": f"Bearer {auth_routine.AuthRoutine.session_tokens[0]}"
        }

        async with session.get(url=f"{auth_routine.AuthRoutine.HOST}produtos/{product["id_bling"]}", headers=headers) as response:
            response_json = await response.json()
            data = response_json["data"]
            data["preco"] = product["divergencias"]["preco"]
            data["pesoLiquido"] = product["divergencias"]["peso"]
            data["pesoBruto"] = product["divergencias"]["peso"]
            data["gtin"] = product["divergencias"]["gtin"]
            data["dimensoes"]["largura"] = product["divergencias"]["largura"]
            data["dimensoes"]["altura"] = product["divergencias"]["altura"]
            data["dimensoes"]["profundidade"] = product["divergencias"]["profundidade"]
            data["tributacao"]["ncm"] = product["divergencias"]["ncm"]

        try:
            async with session.request(method="PUT", url=f"{auth_routine.AuthRoutine.HOST}produtos/{product['id_bling']}", headers=headers, json=data) as resp:
                resp_status = resp.status
                
                if resp_status == 504:
                    product["resposta_api"] = {"response_status": resp_status}
                    logger.error(f"Produto com erro: \n{product}")
                    return product

                if resp_status != 200:
                    product["resposta_api"] = {"decoded_resp": await resp.json(), "response_status": resp_status}
                    logger.error(f"Produto com erro: \n{product}")
                    return product

                product["resposta_api"] = {"response_status":resp_status}
                return product

        except aiohttp.client_exceptions.ClientOSError as err:
            product["resposta_api"] = {"response_status":"OSERROR"}
            logger.error(f"(api_estoque_put) WINERROR: {err.winerror}")
            logger.error("(api_estoque_put) PRODUCT THAT RAISED AN ERROR:")
            logger.error(f"(api_estoque_put) \n{product}")
            logger.exception("ClientOSError")
            return product
        
        except Exception as err:
            #eSTÁ DANDO ERRO 504, CONTENT TYPE ERROR 
            #0, message='Attempt to decode JSON with unexpected mimetype: text/html; charset=utf-8', url=URL('https://www.bling.com.br/Api/v3/produtos/16286903842')
            product["resposta_api"] = {"response_status":"CRITICAL_EXCEPTION"}
            logger.critical("(update_stock_slave) UNHANDLED EXCEPTION")
            logger.exception("(update_stock_slave) UNHANDLED EXCEPTION")
            logger.error(f"(update_stock_slave): \n{product}")
            raise err

    async def update_product_main(self,instructions: list[dict]):

        self.displayer(f"(update_product_main) Começando PUT REQUESTS em {len(instructions)} registros")
        error_not_found: list[dict] = []
        unknown_errors: list[dict] = []
        os_error: list[dict] = []
        ok_status: list[dict] = []
        error_status: list[dict] = []

        batched_diagnosis = list(itertools.batched(instructions, 3))
        async with aiohttp.ClientSession() as session:
            for batch_instructions in batched_diagnosis:
                await asyncio.sleep(1.1)
                tasks = [asyncio.create_task(self.update_product_slave(session,dicts)) for dicts in batch_instructions]
                responses = await asyncio.gather(*tasks)
                
                #ERROR HANDLING
                for resp in responses:
                    
                    if "decoded_resp" in resp["resposta_api"].keys():
                        if "error" in resp["resposta_api"]["decoded_resp"].keys():
                            if resp["resposta_api"]["decoded_resp"]["error"]["type"] == "RESOURCE_NOT_FOUND":
                                error_not_found.append(resp)
                                continue
                            unknown_errors.append(resp)
                    
                    elif resp["resposta_api"]["response_status"] == "OSERROR":
                        os_error.append(resp)
                    elif resp["resposta_api"]["response_status"] == 200:
                        ok_status.append(resp)
                    elif resp["resposta_api"]["response_status"] == 504:
                        unknown_errors.append(resp)

        msg = f"(update_product_main) PUT REQUESTS FINALIZADAS, RESULTADOS:\nerror_not_found:{len(error_not_found)}\nunknown_errors:{len(unknown_errors)}\nos_errors: {len(os_error)}\n"
        self.displayer(msg)
        logger.info(msg)
        
        if error_not_found or unknown_errors or os_error:
            error_status.extend(error_not_found)
            error_status.extend(unknown_errors)
            error_status.extend(os_error)
            logger.warn(f"Os seguintes produtos retornaram com erros:\n{error_status}")
            return (ok_status, error_status)
        return (ok_status, [])

