# DEVLOG

## 09/07/2024

### Primeira Mudança
A função para fazer a request PUT no endpoit idEstoque foi alterada para ser assíncrona. E foi dividida em async_put que prepara os dados para fazer a request, e api_estoque_put que faz a request.

### Segunda Mudança
A verificação da resposta do Banco de Dados foi movida para função verify_db_response, que após o processo de verificação retorna duas listas
has_idestoque e not_idestoque, que divide a resposta baseado em uma condição: se tem idEstoque cadastrado no arquivo bling_product.json ou não.

### Mudanças Planejadas

- A função async_put pode retornar um erro 404 por conta do idEstoque informado ser inválido. Possivelmente por alguém ter excluído o registro em específico. Para contornar esse problema, farei um error handling dentro dessa função para criar um registro novo, caso ele não exista.

- A função api_estoque_post será modificada para ser assíncrona e será retirado dela a obrigação de fazer o registro no arquivo JSON.
