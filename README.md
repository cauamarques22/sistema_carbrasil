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

## 10/07/2024

### Primeira Mudança

A função para fazer a request POST foi alterada para ser assíncrona. Foi dividida em async_post e em api_estoque_post. 

### Segunda Mudança

A função api_estoque_put está lidando com os erros de idEstoque inválido. Caso o idEstoque registrado no JSON não exista no Bling, ele irá criar um novo e alterar o arquivo JSON para o novo ID.