# MC855 - Projeto 1

## Slides:

[Apresentação](https://docs.google.com/presentation/d/1deltK0eSpq3sJCXDTBeXXecgv7sdi0aKxqbySQp86yI/edit?usp=sharing)
[Wikipedia Graph Visualizer](http://luke.deentaylor.com/wikipedia/)

## Objetivos:

* Aprender a utilizar as ferramentas Hadoop e Spark
* Desenvolver um software em cima desse software
* O software deve aplicar MapReduce em uma base de dados e recolher informações importantes dessa base
* A base de dados deve ser grande para poder comparar o MapReduce do Hadoop ao do Spark
* O software deve ser executado em cima de um cluster com data e task management

## Idéia:

>   Criar um software simples (word count com MapReduce) com uma base de dados baseada em texto da Wikipédia e verificar resultados de execução entre Spark rodando em modo **Stand Alone** em single node com ele rodando em modo **cluster YARN** em dual node e verificar diferença de tempo e uso de memória.
>

## Resultados:

>O cluster dual mode teve melhor desempenho de tempo, embora o custo de memória tenha sido maior pela divisão e redundância necessária ao funcionamento do RDD
>
