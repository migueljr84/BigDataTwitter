
# coding: utf-8

# ## <font color='blue'>Miguel Lima Big Data </font>
# 
# ## Download:https://github.com/migueljr84/BigDataTwitter

# ## Stream de Dados do Twitter com MongoDB, Pandas e Scikit Learn

# ## Preparando a Conexão com o Twitter

# In[ ]:


# Instala o pacote tweepy
get_ipython().system('pip install tweepy')


# In[ ]:


get_ipython().system('pip install pymongo')


# In[4]:


# Importando os módulos Tweepy, Datetime e Json
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from datetime import datetime
import json


# In[5]:


# Adicione aqui sua Consumer Key
consumer_key = "XXXXXXX"


# In[6]:


# Adicione aqui sua Consumer Secret 
consumer_secret = "XXXXXXXXXXX"


# In[7]:


# Adicione aqui seu Access Token
access_token = "XXXXXXXX"


# In[8]:


# Adicione aqui seu Access Token Secret
access_token_secret = "XXXXXXXXX"


# In[9]:


# Criando as chaves de autenticação
auth = OAuthHandler(consumer_key, consumer_secret)


# In[10]:


auth.set_access_token(access_token, access_token_secret)


# In[11]:


# Criando uma classe para capturar os stream de dados do Twitter e 
# armazenar no MongoDB
class MyListener(StreamListener):
    def on_data(self, dados):
        tweet = json.loads(dados)
        created_at = tweet["created_at"]
        id_str = tweet["id_str"]
        text = tweet["text"]
        retweet_count = tweet["retweet_count"]
        source = tweet["source"]
        lang = tweet["lang"]
        obj = {"created_at":created_at,"id_str":id_str,"text":text,"created_at":created_at,"retweet_count":retweet_count,"source":source,"lang":lang,}
        tweetind = col.insert_one(obj).inserted_id
        print (obj)
        return True


# In[12]:


# Criando o objeto mylistener
mylistener = MyListener()


# In[13]:


# Criando o objeto mystream
mystream = Stream(auth, listener = mylistener)


# ## Preparando a Conexão com o MongoDB

# In[14]:


# Importando do PyMongo o módulo MongoClient
from pymongo import MongoClient   


# In[15]:


# Criando a conexão ao MongoDB
client = MongoClient('localhost', 27017)


# In[16]:


#client.database_names()
#serverStatusResult=db.command("serverStatus")
#print(serverStatusResult)
# Verificando um documento no collection
# Criando a collection "col"
db = client.twitterdbBotafogo


# In[17]:


# Criando a collection "col"
col = db.tweets 


# In[18]:


# Criando uma lista de palavras chave para buscar nos Tweets
#keywords = ['Jair Bolsonoro','Bolsonaro','jair bolsonaro','jairBolsonaro','jairbolsonaro','Jair bolsonaro']
keywords = ['Botafogo','botafogo','BotaFogo','Botafogo Futebol','Fogão Futebol','Botafogo Futebol e Regatas']


# ## Coletando os Tweets

# In[19]:


# Iniciando o filtro e gravando os tweets no MongoDB
mystream.filter(track=keywords)


# ## --> Pressione o botão Stop na barra de ferramentas para encerrar a captura dos Tweets

# ## Consultando os Dados no MongoDB

# In[20]:


mystream.disconnect()


# In[21]:


# Verificando um documento no collection
col.find_one()


# ## Análise de Dados com Pandas e Scikit-Learn

# In[38]:


# criando um dataset com dados retornados do MongoDB
dataset = [{"created_at": item["created_at"], "text": item["text"], "retweet_count": item["retweet_count"],"source": item["source"],"lang": item["lang"],} for item in col.find()]


# In[39]:


# Importando o módulo Pandas para trabalhar com datasets em Python
import pandas as pd


# In[54]:


#dataset_new = [{"source": item["source"],"lang": item["lang"],} for item in col.find()]
dataset_new = [{"created_at": item["created_at"],"lang": item["lang"],} for item in col.find()]


# In[55]:


#temp = pd.DataFrame({'ticker' : ['spx 5/25/2001 p500', 'spx 5/25/2001 p600', 'spx 5/25/2001 p700']}
df = pd.DataFrame(dataset_new)
df


# In[57]:


import pandas as pd

# Criando um dataframe a partir do dataset 
# convertando o objeto para um Panda
df = pd.DataFrame(dataset_new)
df


# In[58]:


import matplotlib.pyplot as plt
from matplotlib import style
get_ipython().run_line_magic('matplotlib', 'inline')


# In[60]:


# Armazenando o resultado em um Dataframe
purchase_count =df.groupby(["lang"]).count()


# In[61]:


purchase_count


# In[62]:


# O matplotlib.pyplot é uma coleção de funções e estilos que fazem com que o Matplotlib funcione como o Matlab.
import matplotlib as mpl
import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')


# In[63]:


plt.title('Origens do Twitter')
plt.ylabel('Total')
plt.xlabel('Linguagem')
plt.plot(purchase_count)


# In[65]:


# Imprimindo o dataframe
df.duplicated()
average_item_price = purchase_file["Valor"].mean() 


# In[66]:


# Importando o módulo Scikit Learn
from sklearn.feature_extraction.text import CountVectorizer


# In[67]:


# Usando o método CountVectorizer para criar uma matriz de documentos
cv = CountVectorizer()
count_matrix = cv.fit_transform(df.source)


# In[36]:


# Contando o número de ocorrências das principais palavras em nosso dataset
word_count = pd.DataFrame(cv.get_feature_names(), columns=["word"])
word_count["count"] = count_matrix.sum(axis=0).tolist()[0]
word_count = word_count.sort_values("count", ascending=False).reset_index(drop=True)
word_count[:50]


# # Fim