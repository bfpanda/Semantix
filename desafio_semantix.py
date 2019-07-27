#!/usr/bin/env python
# coding: utf-8

# In[28]:


from pyspark import SparkConf, SparkContext
from operator import add
import matplotlib.pyplot as plt
import numpy as np


# In[3]:


conf = (SparkConf()
         .setMaster("local")
         .setAppName("Semantix")
         .set("spark.executor.memory", "2g"))
sc = SparkContext(conf = conf)


# In[4]:


julho = sc.textFile('access_log_Jul95')
julho = julho.cache()


# In[5]:


agosto = sc.textFile('access_log_Aug95')
agosto = agosto.cache()


# Numero de host's unicos

# In[20]:


julhoCount = julho.flatMap(lambda line: line.split(' ')[0]).distinct().count()
agostoCount = agosto.flatMap(lambda line: line.split(' ')[0]).distinct().count()


# In[22]:


objects = ('Julho: %s' % julhoCount, 'Agosto %s' % agostoCount)
x_pos = 100
y_pos = np.arange(len(objects))
performance = [julhoCount, agostoCount]

plt.bar(y_pos, performance, align='center', alpha=0.5)
plt.xticks(y_pos, objects)
plt.ylabel('Numero de Hosts em cada mes')
plt.title('Hosts Unicos')

plt.show()


# Total de erros 404

# In[23]:


def total_error_404(line):
    try:
        code = line.split(' ')[-2]
        if code == '404':
            return True
    except:
        pass
    return False
    
julho_404 = julho.filter(total_error_404).cache()
agosto_404 = agosto.filter(lambda line: line.split(' ')[-2] == '404').cache()

print('Total de erros em Julho: %s' % julho_404.count())
print('Total de erros em Agosto %s' % agosto_404.count())


# Os 5 URLs que mais causaram erro 404

# In[24]:


def erro404top5(rdd):
    erros404 = rdd.map(lambda line: line.split('"')[1].split(' ')[1])
    count_404 = erros404.map(lambda erro404: (erro404, 1)).reduceByKey(add)
    top = count_404.sortBy(lambda pair: -pair[1]).take(5)
    
    print('\nTop 5 dos erros 404 mais frequentes: ')
    for erro404, count in top:
        print(erro404, count)
        
    return top

erro404top5(julho_404)
erro404top5(agosto_404)


# Quantidade de erros 404 por dia

# In[25]:


def count_diario(rdd):
    dias = rdd.map(lambda line: line.split('[')[1].split(':')[0])
    count_dia = dias.map(lambda dia: (dia, 1)).reduceByKey(add).collect()
    
    print('\nErros 404 por dia: ')
    for dia, count in count_dia:
        print(dia, count)
        
    return count_dia

count_diario(julho_404)
count_diario(agosto_404)


# O total de bytes retornados.

# In[26]:


def count_dados_acumulados(rdd):
    def byte_count(line):
        try:
            count = int(line.split(" ")[-1])
            if count < 0:
                raise ValueError()
            return count
        except:
            return 0
        
    count = rdd.map(byte_count).reduce(add)
    return count

print('O total de bytes retornados em Julho: %s' % count_dados_acumulados(julho))
print('O total de bytes retornados em Agosto: %s' % count_dados_acumulados(agosto))


# In[27]:


sc.stop()


# In[ ]:




