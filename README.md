# DEVLOG

## 09/07/2024

### 1
A função para fazer a request PUT no endpoit idEstoque foi alterada para ser assíncrona. E foi dividida em async_put que prepara os dados para fazer a request, e api_estoque_put que faz a request.

### 2
A verificação da resposta do Banco de Dados foi movida para função verify_db_response, que após o processo de verificação retorna duas listas
has_idestoque e not_idestoque, que divide a resposta baseado em uma condição: se tem idEstoque cadastrado no arquivo bling_product.json ou não.

### Mudanças Planejadas

- A função async_put pode retornar um erro 404 por conta do idEstoque informado ser inválido. Possivelmente por alguém ter excluído o registro em específico. Para contornar esse problema, farei um error handling dentro dessa função para criar um registro novo, caso ele não exista.

- A função api_estoque_post será modificada para ser assíncrona e será retirado dela a obrigação de fazer o registro no arquivo JSON.

## 10/07/2024

### 1

A função para fazer a request POST foi alterada para ser assíncrona. Foi dividida em async_post e em api_estoque_post. 

### 2

A função api_estoque_put está lidando com os erros de idEstoque inválido. Caso o idEstoque registrado no JSON não exista no Bling, ele irá criar um novo e alterar o arquivo JSON para o novo ID.

### 3

As constantes foram substituidas por nomes de letra maiúscula.

### 4

Foi adicionado na função async_put um try/except para erros de JSONDecode.

### 5 

Foi adicionado uma verificação, se houver itens na variável "unknown_errors" ele irá mostrar na tela.

### 6

Foi adicionado à função verify_db_response a criação do arquivo de códigos ignorados e verificação de códigos ignorados. 

### 7 

O sistema agora está contando quanto tempo levou para completar o ciclo de sincronização.

### 8 

Foi removido o time.sleep() da função sync_routine, pois com 4.500 produtos o sistema já está demorando 50 minutos para completar o ciclo. Não havendo necessidade de esperar mais, pois assim que for adicionado mais produtos, o sistema demorará mais ainda para completar o ciclo.

### Mudanças Planejadas

- Adição de Type Hints nas variáveis e funções. 
- Adição de comentários para facilitar o entendimento do código.
- Fazer a implementação de Logs para facilitar detecção e solução de erros.
- Melhorar o error handling do sistema para evitar futuras dores de cabeça.