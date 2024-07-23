import asyncio
import logging
import itertools
from queue import Empty

from error_handling import ErrorHandler


logging.basicConfig(level=logging.DEBUG, filemode="a", filename="app_logs.log", format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("request_preprocessing")


class IOHandler(ErrorHandler):

    def __init__(self, txbox, db_sync_instance ,pause_event, stop_event):
        super().__init__(txbox, db_sync_instance)
        self.product_queue = None
        self.yes_stock_queue = None
        self.no_stock_queue = None
        self.db_events = db_sync_instance
        
        self._pause_trigger = pause_event
        self._stop_trigger = stop_event

        self.produtos_atualizados = 0

    async def verify_input(self, diagnosis:list[dict]):

        self.displayer("(verify_input) Separando diagnóstico")
        logger.info("(verify_input) Separando diagnóstico")
        packed_product_info = []
        stock_info = []
        for product in diagnosis:
            on_error_limit = product["internal_error_count"] <= 2
            if product["endpoint_correto"] == "estoque" and on_error_limit:
                stock_info.append(product)

            elif product["endpoint_correto"] == "produto" and on_error_limit:
                if len(packed_product_info) == 3:
                    self.product_queue.put(packed_product_info.copy())
                    packed_product_info.clear()
                packed_product_info.append(product)

            elif product["endpoint_correto"] == "ambos" and on_error_limit:
                stock_info.append(product)

                if len(packed_product_info) == 3:
                    self.product_queue.put(packed_product_info.copy())
                    packed_product_info.clear()
                packed_product_info.append(product)
        
        packed_yes_stock = []
        packed_no_stock = []
        if stock_info:
            for product in self.api_stock_instructions:
                if product["id_estoque"]:
                    if len(packed_yes_stock) == 3:
                        self.yes_stock_queue.put(packed_yes_stock.copy())
                        packed_yes_stock.clear()
                    packed_yes_stock.append(product)
                    continue

                if len(packed_no_stock) == 3:
                    self.no_stock_queue.put(packed_no_stock.copy())
                    packed_no_stock.clear()
                packed_no_stock.append(product)

        logger.debug(f"packed_product_info: \n{packed_product_info}")
        logger.debug(f"stock_info: \n{stock_info}")
        self.displayer("(verify_input) Diagnóstico separado")
        logger.info("(verify_input) Diagnóstico separado")

    async def call_api(self):
        ok_status = []
        error_status = []

        self.displayer("(call_api) Executando funções apropriadas")
        logger.info("(call_api) Executando funções apropriadas")

        product_empty_q_count = 0
        yes_stock_empty_q_count = 0
        no_stock_empty_q_count = 0
        while not self._stop_trigger.is_set():
            self._pause_trigger.wait()
            if product_empty_q_count + yes_stock_empty_q_count + no_stock_empty_q_count == 9:
                break
            try:
                if not self._stop_trigger.is_set() and product_empty_q_count <= 3:
                    product_data = self.product_queue.get(timeout=3)
                    self.displayer("(call_api) Executando 'update_product_main'")
                    logger.info("(call_api) Executando 'update_product_main'")

                    resp1 = await asyncio.create_task(self.update_product_main(product_data))
                    ok_status.append(resp1[0])
                    error_status.append(resp1[1])
                    #( [{}, ...], [{}, ...] )
            except Empty:
                product_empty_q_count+=1
            
            try:
                if not self._stop_trigger.is_set() and yes_stock_empty_q_count <= 3:
                    yes_stock_data = self.yes_stock_queue.get(timeout=3)
                    self.displayer("(call_api) executando 'update_stock_main'")
                    logger.info("(call_api) executando 'update_stock_main'")
                    logger.debug(f"self.yes_stockId: \n{self.yes_stockId}")

                    resp2 = await asyncio.create_task(self.update_stock_main(yes_stock_data))
                    ok_status.append(resp2[0])
                    error_status.append(resp2[1])
                    #( [{}, ...], [{}, ...] )
            except Empty:
                yes_stock_empty_q_count+=1
            
            try:
                if not self._stop_trigger.is_set() and no_stock_empty_q_count <=3:
                    no_stock_data = self.no_stock_queue.get(timeout=3)
                    self.displayer("(call_api) executando 'create_stock_main'")
                    logger.info("(call_api) executando 'create_stock_main'")

                    resp3 = await asyncio.create_task(self.create_stock_main(no_stock_data))
                    ok_status.append(resp3[0])
                    error_status.append(resp3[1])
                    #( [{}, ...], [{}, ...] )
            except Empty:
                no_stock_empty_q_count+=1


        flat_ok = list(itertools.chain.from_iterable(ok_status))
        flat_error = list(itertools.chain.from_iterable(error_status))
        self.produtos_atualizados += len(flat_ok)
        if flat_error:
            await asyncio.create_task(self.error_return_api(flat_error))
        if flat_ok:
            self.db_events.update_mysql_db(flat_ok)

    def run_call_api(self):
        asyncio.run(self.call_api())
    
    def run_verify_input(self, diagnosis):
        asyncio.run(self.verify_input(diagnosis))