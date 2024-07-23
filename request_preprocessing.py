import asyncio
import logging
import itertools

from error_handling import ErrorHandler


logging.basicConfig(level=logging.DEBUG, filemode="a", filename="app_logs.log", format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("request_preprocessing")


class IOHandler(ErrorHandler):

    def __init__(self, txbox, db_sync_instance ,pause_event, stop_event):
        super().__init__(txbox, db_sync_instance)
        self.db_events = db_sync_instance
        self._pause_trigger = pause_event
        self._stop_trigger = stop_event

        self.produtos_atualizados = 0
        self.api_product_instructions = []
        self.api_stock_instructions = []
        self.yes_stockId = []
        self.no_stockId = []
        self.semaphore = asyncio.Semaphore(1)

    def verify_input(self, diagnosis:list[dict]):

        print("(verify_input) Separando diagnóstico")
        logger.info("(verify_input) Separando diagnóstico")
        for product in diagnosis:
            on_error_limit = product["internal_error_count"] <= 2
            if product["endpoint_correto"] == "estoque" and on_error_limit:
                self.api_stock_instructions.append(product)
            elif product["endpoint_correto"] == "produto" and on_error_limit:
                self.api_product_instructions.append(product)
            elif product["endpoint_correto"] == "ambos" and on_error_limit:
                self.api_stock_instructions.append(product)
                self.api_product_instructions.append(product)
        
        if self.api_stock_instructions:
            for product in self.api_stock_instructions:
                if product["id_estoque"]:
                    self.yes_stockId.append(product)
                    continue
                self.no_stockId.append(product)

        logger.debug(f"self.api_product_instructions: \n{self.api_product_instructions}")
        logger.debug(f"self.api_stock_instructions: \n{self.api_stock_instructions}")
        print("(verify_input) Diagnóstico separado")
        logger.info("(verify_input) Diagnóstico separado")

    async def call_api(self):
        ok_status = []
        error_status = []

        print("(call_api) Executando funções apropriadas")
        logger.info("(call_api) Executando funções apropriadas")
        if self.api_product_instructions:
            print("(call_api) Executando 'update_product_main'")
            logger.info("(call_api) Executando 'update_product_main'")

            resp1 = await asyncio.create_task(self.update_product_main(self.api_product_instructions))
            ok_status.append(resp1[0])
            error_status.append(resp1[1])
            #( [{}, ...], [{}, ...] )
        
        if self.yes_stockId:
            logger.info("(call_api) executando 'update_stock_main'")
            logger.debug(f"self.yes_stockId: \n{self.yes_stockId}")

            resp2 = await asyncio.create_task(self.update_stock_main(self.yes_stockId))
            ok_status.append(resp2[0])
            error_status.append(resp2[1])
            #( [{}, ...], [{}, ...] )
        if self.no_stockId:
            logger.info("(call_api) executando 'create_stock_main'")
            logger.debug(f"self.no_stockId: \n{self.no_stockId}")

            resp3 = await asyncio.create_task(self.create_stock_main(self.no_stockId))
            ok_status.append(resp3[0])
            error_status.append(resp3[1])
            #( [{}, ...], [{}, ...] )

        self.api_product_instructions.clear()
        self.api_stock_instructions.clear()
        self.yes_stockId.clear()
        self.no_stockId.clear()

        flat_ok = list(itertools.chain.from_iterable(ok_status))
        flat_error = list(itertools.chain.from_iterable(error_status))
        
        if flat_error:
            self.error_return_api(flat_error)
        if flat_ok:
            self.produtos_atualizados += len(flat_ok)
            self.db_events.update_mysql_db(flat_ok)


    def main(self):
        asyncio.run(self.call_api())
