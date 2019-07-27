# Semantix

### Desafio Engenheiro de Dados

1 - Qual o objetivo do comando cache em Spark?
 - O objetivo do comando é a otimização de uso de disco, o termo In-Memory Computation ja descreve, pois quando o utilizamos armazenamos os RDD's em memória assim todo o uso sera em momória, sem utilizar o disco obtemos um melhor desempenho, podemos fazer a transformações em diversas RDD's até chamar uma action que ai sim irá salvar em disco o resultado das transformações, podemos chamar o processo de Lazy Evaluation, onde as transformações em diversos RDD's ocorrem de forma preguiçosa e a partir de uma ação somente os valores necessários (resultados) são gravados em disco.

2 - O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê?
- Pelo motivo citado na pergunta acima, os jobs executados em MapReduce usarão disco assim quando um job termina e seu resultado escrito em disco outro job quando chamado deverá ser lido e escrito em disco novamente, ja no caso do Spark com o uso de caching conseguimos fazer um "processamento in-memory" até guardar o resultado final em disco.

3 - Qual é a função do SparkContext?
- Bom, para falar que uma aplicação Spark é uma aplicação Spark, creio tem que ter o uso do sparCOntext meio que por padrão, pois sem ele não há a comunicação com o gerenciador de cluster, sendo assim sua funcionalidade nada mais é que ser a famosa porta de entrada da aplicação Spark com o cluster através do Yarn ou qualquer outro gerenciador, e depois de criado ai sim podemos usa-lo para criar as RDD's.

4 - Explique com suas palavras o que é Resilient Distributed Datasets (RDD).
- As RDD's como ja citada algumas vezes acima, assim como seu próprio nome ja diz, é um conjunto de dados tolerantes a falha que seráo processados de forma distribuida, seus valores é dividido em tipos, como string, pares, inteiros e etc... RDD's são imutaveis sendo assim vc cria uma e vai transformando e criando outra transformando e tudo isso de forma distribuida podendo fazer diversas transformações em simultâneo e em In-Memory utilizando apenas a memória para obter os resultados e quando tomada uma ação sobre isso salvar seus resultados em disco. 
5 - GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?
- Porque utilizando o GroupByKey() iremos ter um monte de dados embaralhados desnecessários passando pela rede e ainda correr o risco de ter um erro de out of memory caso uma das chaves passar o valor maior que o tamanho que possa caber na memoria, pensando em desempenho o ReduceByKey() será muito mais viavel pois ele primeiro irá combinar a saída com as chaves em comum em cada partição, assim com o uso de uma função lambda os valores de cada partição são reduzidos para produzir resultado.

6 - Explique o que o código Scala abaixo faz:
# val textFile = sc.textFile("hdfs://...")
- Arquivo sendo lido

# val counts = textFile.flatMap(line => line.split(" ")) 
- Quebra a linha em palavras e as sequencias de cada linha serão agrupadas num conjuto de palavras

# .map(word => (word, 1)) 
- transforma cada palavra num mapeamento onde a propria palacra é a chave e o valor 1

# .reduceByKey(_ + _) 
- Valores são agregados através do +

# counts.saveAsTextFile("hdfs://...")
- RDD é salvo com a contagem de cada palavra num arquivo de texto
