
#Importation d'un fichier csv
data=	pd.read_csv(gama)
 

#Importation d'un fichier excell 
import pandas as pd
results=pd.read_excel("D:\FErman\Desktop\pythonxx.xlsx")
results.head()
#Si plusieurs feuille
results=pd.read_excel("D:\FErman\Desktop\pythonxx.xlsx",sheetname="..")
results.head()

#Manipule des data
data.shape 		#donne nombre de lignes et de colonnes

data.columns 	#donne le nom des colonnes

print(data["nomVariable"]) # donne le nom de la variable

#Faire une concaténation entre plusieurs tables
In [4]: frames = [df1, df2, df3]

In [5]: result = pd.concat(frames)




############################################################################
###Création d'une table avec ajout d'une colonne
import pyspark.sql.functions as func 
from pyspark.sql.window import Window 
from pyspark.sql.types import IntegerType

spark.sql("use default")
sqlContext.sql("show tables").show()

df=spark.sql("select * from default.fer_ref_regionfrance")
df

#On crée une boucle 
list_code=["Bretagne","Occitanie"]
for i in range(len(list_code)):
    if i==0:
        df = df.withColumn('flag',func.when(df.region_de_france==list_code[i],"ok").otherwise("0"))
    else:
        df = df.withColumn('flag',func.when((df.region_de_france==list_code[i])|(df.flag=="ok") ,"ok"))

#On est obligé de créer une table temporaire à partir de notre table existante df
df.registerTempTable("dataframe")


#On a crée une table dans le schema default
spark.sql("use default")
spark.sql("create table FER_dataframe as select * from dataframe")
#################################################################################






#################################################################################
### exemple trouve sur stackoverflow : If these are RDDs you can use SparkContext.union method:

rdd1 = sc.parallelize([1, 2, 3])
rdd2 = sc.parallelize([4, 5, 6])
rdd3 = sc.parallelize([7, 8, 9])

rdd = sc.union([rdd1, rdd2, rdd3])
rdd.collect()

## [1, 2, 3, 4, 5, 6, 7, 8, 9]

There is no DataFrame equivalent but it is just a matter of a simple one-liner:

from functools import reduce  # For Python 3.x
from pyspark.sql import DataFrame

def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)

df1 = sqlContext.createDataFrame([(1, "foo1"), (2, "bar1")], ("k", "v"))
df2 = sqlContext.createDataFrame([(3, "foo2"), (4, "bar2")], ("k", "v"))
df3 = sqlContext.createDataFrame([(5, "foo3"), (6, "bar3")], ("k", "v"))

unionAll(df1, df2, df3).show()

## +---+----+
## |  k|   v|
## +---+----+
## |  1|foo1|
## |  2|bar1|
## |  3|foo2|
## |  4|bar2|
## |  5|foo3|
## |  6|bar3|
## +---+----+
#################################################################################







##################################################################################
#Creation d'une table 

from pyspark.sql import DataFrame
df1=sqlContext.createDataFrame([("pharmacie",7608),("vpc",4578),("hut",8954),("electromenager",4567),("electromenager",4561)] , ("famille_mcc","code_mcc"))
####################################################################################







##################################################################################
#Pour #filtrer
>>> df.filter(df.age > 3).collect()
[Row(age=5, name=u'Bob')]
>>> df.where(df.age == 2).collect()
[Row(age=2, name=u'Alice')]

>>> df.filter("age > 3").collect()
[Row(age=5, name=u'Bob')]
>>> df.where("age = 2").collect()
[Row(age=2, name=u'Alice')]
#################################################################################

 





###############################################################################
#faire une #jointure, à noter que le inner peut changer. 
(df=spark.sql('select * from default.fer_datawall_famillemcc_pdm_proxia')
df.count()
df.distinct().count()
df.select('mcc').distinct().show()

df2=spark.table("default.FER_FAMILLE_MCC")

df.join(df2,df['mcc']==df2['mcc'],"inner")
join()
#################################################################################





################################################################################
#pour rechercher une valeur dans un champ de valeur

df1.show()
+--------------+--------+
|   famille_mcc|code_mcc|
+--------------+--------+
|     pharmacie|    7608|
|           vpc|    4578|
|           hut|    8954|
|electromenager|    4567|
|electromenager|    4561|
+--------------+--------+


import pyspark.sql.functions as func
df1.where(func.col("famille_mcc").like("%harm%")).show()
+-----------+--------+
|famille_mcc|code_mcc|
+-----------+--------+
|  pharmacie|    7608|
+-----------+--------+
###############################################################################






#############################################################################
(df=spark.sql('select * from default.fer_datawall_famillemcc_pdm_proxia')
df.count()
df.distinct().count()
df.select('mcc').distinct().show

df2=spark.table("default.FER_FAMILLE_MCC")

df.join(df2,df['mcc']==df2['mcc'],"inner")
join()


)

#############################################################################
Pour Python: 
#Pour convertir la valeur d'une colonne vers le integer
print (pd.to_numeric(df.b, errors='coerce'))
#############################################################################


############################################################################
#Pour filtrer avec plusieurs conditions
f = pd.DataFrame({'Def':[True] *2 + [False]*4,'days since':[7,8,9,14,2,13],'bin':[1,3,5,3,3,3]})


temp2 = df[~df["Def"] & (df["days since"] > 7) & (df["bin"] == 3)]
print (temp2)

     Def  bin  days since
3  False    3          14
5  False    3          13




temp2 = f[ (f["days since"] > 7) & (f["bin"] == 3)]
print (temp2)
temp2 = f[ (f["days since"] > 7) & (f["bin"] == 3)]

print (temp2)

     Def  bin  days since
1   True    3           8
3  False    3          14
5  False    3          13

############################################################################




############################################################################
#equivalent du select in list like('%zer%')
df.select('c43_nom_et_adresse_de_laccepteur_de_carte').where(func.col('c43_nom_et_adresse_de_laccepteur_de_carte').like("%CDISCOUNT%")).show()

############################################################################



#############################################################################

In [6]: df=pd.DataFrame({'Year':['2014', '2015'], 'quarter': ['q1', 'q2']})

In [7]: df
Out[7]:
   Year quarter
0  2014      q1
1  2015      q2

In [8]: df['period'] = df[['Year', 'quarter']].apply(lambda x: ''.join(x), axis=1)

In [9]: df
Out[9]:
   Year quarter  period
0  2014      q1  2014q1
1  2015      q2  2015q2


#############################################################################

###jointure python###

person_likes = [{'person_id': '1', 'food': 'ice_cream', 'pastimes': 'swimming'},
                {'person_id': '2', 'food': 'paella', 'pastimes': 'banjo'}]

person_accounts = [{'person_id': '1', 'blogs': ['swimming digest', 'cooking puddings']},
                   {'person_id': '2', 'blogs': ['learn flamenca']}]

### Effectuer la jointure ###
import pandas as pd
accounts = pd.DataFrame(person_accounts)
likes = pd.DataFrame(person_likes)


pd.merge(accounts, likes, on='person_id',how='inner')

#Il faut définir la Dataframe avec pandas pour pouvoir faire la jointure entre deux tables 
#on peut soit la définir au moment de création de table soit apres.

How much does it cost to write the real investment 


#Pour le toPandas(), cela ramene les données en local et les sort de spark. 
#Il faut donc les sortir de Spark

94

#################
#conversion d'une table en dataframe et creation de table sur default de big data
from pyspark.sql import SparkSession
from pyspark import SparkContext
df2=spark.createDataFrame(df)  pyspark
df2.registerTempTable("test")
spark.sql("use default")
spark.sql ('create table fer_donnees_meteo as select * from test')
# Ensuite dans impala faire un INVALIDATE METADATA default.fer_donnees_meteo pour valider la création de la donnée

###################################

#Conversion 
#Sur Python on a soit une dataframe soit une serie 

###################################
#Pour renommer une colonne :
df.rename(index=str, columns={"A": "a", "B": "c"})




############################################
import pandas as pd
df = pd.DataFrame({ 'gene':["1 // foo // blabla",
                                   "2 // bar // lalala",
                                   "3 // qux // trilil",
                                   "4 // woz // hohoho"], 'cell1':[5,9,1,7], 'cell2':[12,90,13,87]})
df = source_df[["gene","cell1","cell2"]]


#Pour créer trois colonnes
In [13]:
df['gene'] = df['gene'].str.split('//').str[1]
df

Out[13]:
   cell1  cell2   gene
0      5     12   foo 
1      9     90   bar 
2      1     13   qux 
3      7     87   woz 

#######################################
#Pour renommer dans une dataframe pyspark :
df = df1.selectExpr("Name as name")

#Pour renommer dans une dataframe pandas :
df1.rename(index=str, columns={"c43_nom_et_adresse_de_laccepteur_de_carte":A})



#######
#Il existe des dataframe pandas et des dataframe pyspark/sparK faire attention à ne pas confondre
# les deux 

#Pour changer format d'une variable en dataframe pyspark.
df['name']=df['name'].astype(str)

#Pour créer une colonne à partir des valeurs des autres colonnes
def label_race (row):
   if row['eri_hispanic'] == 1 :
      return 'Hispanic'
   if row['eri_afr_amer'] + row['eri_asian'] + row['eri_hawaiian'] + row['eri_nat_amer'] + row['eri_white'] > 1 :
      return 'Two Or More'
   if row['eri_nat_amer'] == 1 :
      return 'A/I AK Native'
   if row['eri_asian'] == 1:
      return 'Asian'
   if row['eri_afr_amer']  == 1:
      return 'Black/AA'
   if row['eri_hawaiian'] == 1:
      return 'Haw/Pac Isl.'
   if row['eri_white'] == 1:
      return 'White'
   return 'Other'

   df.apply (lambda row: label_race (row),axis=1)

  # puis si on veut créer une colonne avec 
   df['race_label'] = df.apply (lambda row: label_race (row),axis=1)


   #je converties en dataframe pyspark
spark.createDataFrame(df1).show()




############################################################
#get dummies for event columns
calendar=pd.get_dummies(calendar,columns=['event_type_1',"event_type_2"],dummy_na=True)
############################################################

############################################################
# create new features
def label_race(row):
    if row['event_type_1_nan'] + row['event_type_2_nan']>=1:
        return '1'
    if row['event_type_1_nan'] + row['event_type_2_nan']==0:
        return "0"

def label_roll(row):
    if row['event_type_1_Cultural'] + row['event_type_2_Cultural']>=1:
        return "1"
    if row['event_type_1_Cultural'] + row['event_type_2_Cultural']==0:
        return "0"

def label_step(row):
    if row['event_type_1_Religious'] + row['event_type_2_Religious']>=1:
        return "1"
    if row['event_type_1_Religious'] + row['event_type_2_Religious']==0:
        return "0"


calendar["No_event"]=calendar.apply(lambda row: label_race(row),axis=1)
calendar["event_type_Cultural"]=calendar.apply(lambda row: label_roll(row),axis=1)
calendar["event_type_Religious"]=calendar.apply(lambda row: label_step(row),axis=1)
############################################################


############################################################
#rename columns
calendar.rename(columns={"event_type_1_Sporting":"event_type_Sporting"},inplace=True)
############################################################

############################################################
# The number of weekday has been replaced by a week number day beginning with monday as 0 and sunday as 6
calendar.loc[:,'date']=pd.to_datetime(calendar["date"],format="%Y-%m-%d")
calendar["wday"]=calendar["date"].dt.weekday
calendar
############################################################



#####################################################
#To avoid high use of resources we change the name of the days columns and replace it by date
# En gros on remplace nom des colonnes par nom dans une liste 
a=calendar["date"]
b=list(a)

#%%

c=sales_train_validation
c.head(2)

#%%

c=c.iloc[:, 6:]


#%%

# replace old name by new name (here c by b)
c.rename(columns={i:j for i,j in zip(c,b)}, inplace=True)


#Convert the day columns to rows. Contrary of dummies. First attempt on the 5 first row.
sales_train_validation_5=sales_train_validation_5.melt(id_vars=["id", "item_id","dept_id","cat_id","store_id", "state_id"],
        var_name="d",
        value_name="Value")
############################################################


############################################################
#convert d to datetime
sales_train_validation.loc[:,'d']=pd.to_datetime(sales_train_validation["d"],format="%Y-%m-%d")
#get month and year of each sample
sales_train_validation['month']=sales_train_validation['d'].dt.month
sales_train_validation['year']=sales_train_validation['d'].dt.year


#create weekday column
sales_train_validation['wday']=sales_train_validation['d'].dt.weekday
############################################################



############################################################
#merge
WI_1=pd.merge(WI,calendar_x2,how="left",on="d")
############################################################

############################################################
#Add id feature
sell_prices1["id"] =sell_prices1["item_id"] +"_"+ sell_prices1["store_id"]+"_validation"
sell_prices1.head(2)
############################################################

############################################################
#avoir les valeurs uniques d'une colonne equivalent select distinct
sell_prices['id'].nunique()
############################################################

############################################################
##Convert dummies
WI_4=pd.get_dummies(WI_3,columns=["store_id","cat_id","dept_id","state_id"])
############################################################

############################################################
#Calcul of the missing values
x1_missing_values=WI_4.isnull()
#No missing values
for column in x1_missing_values.columns.values:
    print(column)
    print (x1_missing_values[column].value_counts())
    print("")
############################################################




############################################################
    # KNN IMPUTER 
    import numpy as np
import pandas as pd
dict = {'First':[np.nan, np.nan,np.nan, np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,
                 np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,9,12,65,75],
        'Second': [30, 45, 56, 45,35,78,52,61,14,102,102,30, 45, 56, 45,35,78,52,61,14],
        'Third':[50, 40, 80, 98,54,62,14,75,35,150,40, 80, 98,54,62,14,102,102,30, 45]}
df = pd.DataFrame(dict)
# creating a dataframe from list
df
#%%
imputer = KNNImputer(n_neighbors=4)
dfx=imputer.fit_transform(df)
dfx
#%%
d fx1=pd.DataFrame(dfx)
dfx1
#%%
WI_4=pd.DataFrame(WI_4)
#%%
dfx=imputer.fit_transform(WI_4)
###########################################################################


############################################################
#faire un group by 
WI_4[WI_4["year"]==2014] .groupby(['year'])['sell_price'].mean()
############################################################

############################################################
# est dans une liste
years=['2011','2012']
WI_4=WI_4.loc[WI_4['year'].isin (years)]
#est pas dans une liste
years=['2011','2012']
WI_4=WI_4.loc[~WI_4['year'].isin (years)]
############################################################

############################################################
#transform : créer directement une colonne en faisant par exemple ici la moyenne par rapport 
#au group by
WI_4["sell_prices_mean_month_year"]=WI_4.groupby(["month","year"])["sell_price"].transform("mean")
############################################################

############################################################
#correlation
#%%
corr_matrix=WI_4.corr()
corr_matrix
#%%
corr_matrix["Value"].sort_values(ascending=False)
############################################################


############################################################
#Exporter une dataframe en csv sur Pycharm
sales_train_validation.to_csv("venv/data/sales_train_validation.csv")
############################################################


############################################################
#Find the maximum among the given numbers
#The max() function has two forms:
#// to find the largest item in an iterable
#max(iterable, *iterables, key, default)

#// to find the largest item between two or more objects
#max(arg1, arg2, *args, key)
 #EXAMPLE 1 : plus grosse valeur 
max(4, -5, 23, 5)
result = 23 

#EXAMPLE 2: the largest string in a list
languages = ["Python", "C Programming", "Java", "JavaScript"]
largest_string = max(languages);
print("The largest string is:", largest_string)
The largest string is: Python
############################################################


############################################################
#f-Strings: A New and Improved Way to Format Strings in Python
>>> name = "Eric"
>>> age = 74
>>> f"Hello, {name}. You are {age}."
'Hello, Eric. You are 74.'

>>> f"{2 * 37}"
'74'

>>> numcols = [f"d_{day}" for day in range(start_day,tr_last+1)]
############################################################


############################################################
#update() Method
#nsert an item to the dictionary
car = {
  "brand": "Ford",
  "model": "Mustang",
  "year": 1964
}

car.update({"color": "White"})

print(car)
############################################################
azeazeaze
dsqdqsd