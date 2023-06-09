# Bibliothèques de base
import pandas as pd
from collections import Counter
from statistics import mean 
from datetime import datetime
import numpy as np

# Bibliothèques d'analyse de réseau
import networkx as nx
from ipysigma import Sigma
import networkx.algorithms.community as nx_comm
#import community

# Bibliothèques pour la construction de graphique
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
import matplotlib.dates as mdates

# Extraction des racines URLs
from urllib.parse import urlparse



### FONCTIONS D'ANALYSE DES PERSONNES CENTRALES  ##############################################################

# num_cluster = l'identifiant du cluster souhaité / nb = le nombre d'éléments retournés

def centralite_betweeness(num_cluster, n , graph, list_clusters):
    """
    Identifier les points n les plus centraux (centralité d'intermediarité) dans le cluster num_cluster
    de la liste cluster_multiples du graph
    """
    sub = graph.subgraph(list_clusters[num_cluster])
    result = sorted(nx.betweenness_centrality(sub, k=150).items(), 
                     key=lambda x:x[1], reverse=True)[0:n]
    result = pd.DataFrame(result, columns=['Compte', 'Centralité'])
    
    return result.round(2)

# Fonction pour identifier les n personnes avec le plus de liens (de RT) du cluster Y (num_cluster)
def centralite_betweeness_bi(num_cluster, n , graph, list_clusters):
    """
    Identifier les points n les plus centraux (centralité d'intermediarité) dans le cluster "num_cluster"
    issu de la bipartition du graph
    """
    sub = graph.subgraph(list_clusters[num_cluster])
    result = sorted(nx.betweenness_centrality(sub, k=150).items(), 
                     key=lambda x:x[1], reverse=True)[0:n]
    result = pd.DataFrame(result, columns=['Compte', 'Centralité'])
    
    return result.round(2)



### FONCTIONS D'ANALYSE DES RETWEETS PLEBISCITES DES CLUSTERS ************************************************************************

# Fontion pour identifier de quels clusters viennent les tweets Retweetés par le cluster Y (num_cluster)
def retweets_from(num_cluster,nb, data):
    """
    Identifier les comptes d'origines des tweets retweets par le cluster étudié
    """
    
    # Nombre de RT fait par le cluster étudié
    integralite_RT = data[data.Cluster == num_cluster]['RT_nb'].sum()
    
    # Nombre de RT fait par le cluster étudié de chaque autre utilisateur du réseau
    cluster_RT = data[data.Cluster == num_cluster].groupby('retweeted_user_name')['RT_nb'].sum().sort_values(ascending=False)
    
    # On calcul le ratio
    result = round(cluster_RT / integralite_RT,2).head(n=nb)
    
    result = pd.DataFrame(result)
    
    return result


# Fontion pour identifier les tweets (les textes) les plus Retweetés par le cluster étudié (num_cluster)
def retweets(num_cluster,nb,data):
    """
    Identifier les textes des tweets retweets par le cluster étudié
    """
    result = data[data.retweeted_cluster_name == num_cluster].groupby('text')['RT_nb'].sum()
    
    result = pd.DataFrame(result)
    
    return result.sort_values(ascending=False)[0:nb]






### FONCTIONS pour extraire les hashtags les plus populaires de chaque cluster  #####################################################

# Spécialement pour la bipartition
def hashtags_bipartition(num_cluster, nb, df, fichier_correction):
    """
    Calculer la fréquence des hashtags des cluster issus de la bipartition: Pro et Anti
    """
    
    # Extraction des hashtags du Cluster 'num_cluster'
    hashtag = list(pd.Series([j for i in list(df[df.Cluster_2class == num_cluster]["hashtags"].dropna().apply(lambda x : x.lower().split("|"))) for j in i]))
    
    # Harmonisation de l'hortographe des hahstags
    for index, h_bon in fichier_correction.iterrows():
        hashtag = [word if word != h_bon['Originaux'] else h_bon['Remplacement'] for word in hashtag]
    hashtag = list(map(lambda x: x.replace('covidー19', 'Covid-19'), hashtag))
    
    # On retourne le nombre des 'nb' plus fréquents hashtags
    result = pd.DataFrame(Counter(hashtag).most_common(nb),columns=['hahstags', 'Nombre de partage'])
    
    return result


    
# Pour la multipartition: Même chose mais ici on regarde pour chacun des 7 clusters issus de la multipartition.
def hashtags_multipartition(num_cluster,nb, df, fichier_correction):
    """
    Calculer la fréquence des hashtags des cluster issus de la multipartition
    """
    
    # Extraction des hashtags du Cluster 'num_cluster'
    hashtag = list(pd.Series([j for i in list(df[df.Cluster == num_cluster]["hashtags"].dropna().apply(lambda x : x.lower().split("|"))) for j in i]))
    
    # Harmonisation de l'hortographe des hahstags
    for index, h_bon in fichier_correction.iterrows():
        hashtag = [word if word != h_bon['Originaux'] else h_bon['Remplacement'] for word in hashtag]
    hashtag = list(map(lambda x: x.replace('covidー19', 'Covid-19'), hashtag))
    
    # On retourne le nombre des 'nb' plus fréquents hashtags
    result = pd.DataFrame(Counter(hashtag).most_common(nb),columns=['hahstags', 'Nombre de partage'])
    
    return result

 

def hashtags_multipartition_spe(num_cluster, df, fichier_correction, hashtag_total): 

# On créé la fonction qui compare le cluster choisi à la moyenne. Il classifie les hashtags les plus surmentionnés et sous mentionnés.
    
    # 1: On extrait les 50 plus fréquents de l'ENSEMBLE DU CORPUS. Ce tableau servira de base de comparaison
    hashtags_corpus = pd.DataFrame(Counter(hashtag_total).most_common(50), columns=['h', 'nb'])

    # 2: Extraction des hashtags du Cluster 'num_cluster'
    hashtag = list(pd.Series([j for i in list(df[df.Cluster == num_cluster]["hashtags"].dropna().apply(lambda x : x.lower().split("|"))) for j in i]))
    
    # 2 Harmonisation de l'hortographe des hahstags
    for index, h_bon in fichier_correction.iterrows():
        hashtag = [word if word != h_bon['Originaux'] else h_bon['Remplacement'] for word in hashtag]
    hashtag = list(map(lambda x: x.replace('covidー19', 'Covid-19'), hashtag))
    
    # 2 On sauvegarde les hahstags extraits dans un dataframe
    hashtag_cluster = pd.DataFrame(Counter(hashtag).most_common(50), columns=['h', 'nb']) # Le nombre de # comparé est un paramètre
    
    
    # 3:  On réalise plusieurs comparaison avec l'ensemble du corpus (hashtags_corpus)   
    tableau = pd.DataFrame(columns=['h', '%_Tous#', '%_du#', 'dif_%', 'ecart_%tweet'])
    # 'h': Le hashtag
    # '%_Tous#': la proportion du hashtag par rapport à l'ensemble des hahstags mentionnés par le cluster.
    # '%_du#': la part des mentions du Hashtags X par le Cluster Y (Ex: "#Covid-19" est mentionné dans 50% des cas par le Cluster Y)
    # 'dif_%liens': L'écart entre la part que représente le Hashtag X pour le Cluster Y par rapport à la part total du Hashtag X
    # 'diff_%#': la part des tweets fait le Cluster X comparée avec '%_du#'
        
    # On créé la boucle où on stock ces informations
    for index, row in hashtag_cluster.iterrows():
        for index2, row2 in hashtags_corpus.iterrows():
            if row['h'] == row2['h']:
                new_row = {'h': row['h'], 
                           '%_Tous#': round( (row['nb']/len(hashtag))*100 ,1) , 
                           '%_du#': round((row['nb']/row2['nb'])*100,2) , 
                           'dif_%': round(( (row['nb']/len(hashtag)) - (row2['nb'] / len(hashtags_corpus) ) )*100,1) , 
                           'ecart_%tweet': round( ( (row['nb']/row2['nb']) - (len(hashtag) / len(hashtags_corpus) ) )*100, 1) }
                tableau = tableau.append(new_row, ignore_index=True)
    
    # On stock les réponses dans des dataframe où l'on ne garde que les 5 Hashtags les PLUS et les MOINS sépcifiques du cluster.
    tableau = tableau.sort_values(by=['ecart_%tweet'], ascending=False)
    tableau_final = tableau.head(5).append(tableau.tail(5))
    
    # On affiche le résultat final
    return tableau_final






### FONCTIONS pour extraire les liens spécifiques de chaque cluster ##############################################################

# Nous devons d'abord extraire tous les liens partagés

# Puis on créé une fcontion qui va extraire les liens du cluster désiré et réaliser une comparaison

def liens_bipartition_spe(num_cluster, df, url_domaine_total):
    
    # 1: On extrait les 50 plus fréquents de l'ENSEMBLE DU CORPUS. Ce tableau servira de base de comparaison 
    url_corpus = pd.DataFrame(Counter(url_domaine_total).most_common(50),columns=['Site', 'freq'])
    
    # 2: Extraction des racines des liens urls du cluster 'num_cluster'
    url_domaine = list(pd.Series([(urlparse(str(j)).netloc) for i in list(df[df.Cluster_2class == num_cluster]["links"].dropna().apply(lambda x : x.lower().split("|"))) for j in i]))
    
    # Nettoyage des liens: Suppression des liens twitter
    url_domaine = [value for value in url_domaine if value != "twitter.com"]
    url_domaine = list(map(lambda x: x.replace('amp-theguardian-com.cdn.ampproject.org', 'www.theguardian.com'), url_domaine))
    
    # On garde les 50 racines URLs les plus partagés
    url_domaine_cluster = pd.DataFrame(Counter(url_domaine).most_common(50),columns=['Site', 'freq'])
    
    # 3: On compare avec l'ensemble du corpus
    tableau = pd.DataFrame(columns=['Site', '%_cluster', '%_liens', 'dif_%liens', 'dif_%total'])
    # Site : Les liens
    # %_cluster: Proportion du lien par rapport à l'ensemble des liens mentionnés par le cluster
    # %_liens: La part total des mentions du liens X par le Cluster Y (Ex:"YYY.fr" est mentionné dans 50% des cas par le Cluster Y)
    # dif_%liens: L'écart entre la part que représente le lien X pour le Cluster Y par rapport à la part du lien X de tous les liens
    # dif_%total: écart entre la part des tweets fait le Cluster X et %_liens
    
    # On créé la boucle où on stock ces informations
    for index, row in url_domaine_cluster.iterrows():
        for index2, row2 in url_corpus.iterrows():
            if row['Site'] == row2['Site']:            
                new_row = {'Site': row['Site'], 
                           '%_cluster':round( (row['freq']/len(url_domaine))*100 ,1) , 
                           '%_liens': round((row['freq']/row2['freq'])*100,2) , 
                           'dif_%liens': round(( (row['freq']/len(url_domaine)) - (row2['freq'] / len(url_domaine_total) ) )*100,1) , 
                           'dif_%total':round( ( (row['freq']/row2['freq']) - (len(url_domaine) / len(url_domaine_total) ) )*100, 1) }
                tableau = tableau.append(new_row, ignore_index=True)
    
    # 4: On stock les réponses dans des dataframe où l'on ne garde que les 5 Hashtags les PLUS et les MOINS sépcifiques du cluster.
    tableau = tableau.sort_values(by=['dif_%total'], ascending=False)
    tableau_final = tableau.head(8).append(tableau.tail(8))
    
    # On affiche le résultat final
    return tableau_final