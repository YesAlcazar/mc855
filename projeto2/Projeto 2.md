# MC855 - Projeto 2

## Slides:

[Apresentação](https://docs.google.com/presentation/d/1FbQ6B7hZ8mGOq0Z_oSXnWARg6a7kkocE_-cdgnFcyLU/edit?usp=sharing)

## Objetivos:

* Explorar ferramentas do Spark e compreender em totalidade as funcionalidades mais básicas
* Entender como as bibliotecas de manipulação e persistência de dados funcionam
* Simular um cluster mais complexo, com 4 nodes
* Testar diferentes configurações de YARN
* Realizar Data Mining em Big Data fazendo uso do Spark

## Idéia:

>Montar um grafo de conexão de palavras nas línguas portuguesa, inglesa, alemã e francesa, baseado em páginas da Wikipédia.
>

## Resultados:

>Projeto de objetivo diferente, mas com grafo próximo ao obtido: [Wikipedia Map](http://luke.deentaylor.com/wikipedia/). A base de dados foi obtida via download, um job que durou 20 dias. Enquanto baixava o Spark lia os arquivos e criava o link entre eles em outra thread. (Ambos nós baixavam e e manipulavam, compartilhando os dados)
>
>
