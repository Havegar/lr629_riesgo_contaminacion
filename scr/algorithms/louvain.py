
### Algoritmo de Louvain y su aplicacion a la red de fuerzas para la deteccion de comunidades mediante maximizaicon de la modularidad.

import pandas as pd
import networkx as nx
from cdlib import algorithms
import os
from networkx.algorithms.community import modularity



# Cargar datos desde el archivo CSV de la combinacion de ambas fuerzas
df_relaciones = pd.read_csv("/home/cdsw/data/processed/fuerza_iva_representante.csv")

# Crear un grafo dirigido ponderado, donde el ponderador sera la fuerza de la relacion
G = nx.DiGraph()
for _, row in df_relaciones.iterrows():
    G.add_node(row['emisor'])
    G.add_node(row['receptor'])
    G.add_edge(row['emisor'], row['receptor'], fuerza=row['Fi'])

# Convertir el grafo dirigido a un grafo no dirigido ponderado por la fuerza de la relacion
H = nx.Graph(G)

for u, v, data in G.edges(data=True):
    if H.has_edge(u, v):
        H[u][v]['fuerza'] += data['fuerza']
    else:
        H.add_edge(u, v, fuerza=data['fuerza'])
        

# Aplicar el algoritmo de Louvain con peso correspondiente, maximizando la modularidad de la red.
# Un valor de resolución cercano a 0 tiende a producir comunidades más grandes y menos densas. En este caso, el algoritmo # prioriza la detección de comunidades más amplias con conexiones más dispersas.

# Un valor de resolución cercano a 1 tiende a producir comunidades más pequeñas y más densas. En este caso, el algoritmo # prioriza la detección de comunidades más específicas con conexiones más densas entre sus nodos.
louvain_result = algorithms.louvain(H, weight="fuerza", resolution=0.5)

# Calcular la modularidad utilizando la función modularity de networkx. La modularidad es unica en la particion de la red.
modularity_value = modularity(H, louvain_result.communities, weight="fuerza")

# Crear un diccionario para mapear nodos a comunidades
node_community_mapping = {node: idx + 1 for idx, community in enumerate(louvain_result.communities) for node in community}

# Crear un DataFrame para mostrar nodos, comunidades y modularidad
df_comunidades = pd.DataFrame(list(node_community_mapping.items()), columns=['nodo', 'comunidad'])
df_comunidades['modularidad'] = modularity_value

# Leer el DataFrame de contaminados y labels para agregar la comunidad correspondiente a cada entidad
existing_dataframe_path = '/home/cdsw/data/processed/contaminados_processed_iva_representante_total_with_labels.csv'
df_existente = pd.read_csv(existing_dataframe_path)


# Fusionar DataFrames utilizando la columna 'nodo' y 'cont_rut'
df_final = pd.merge(df_existente, df_comunidades, left_on='cont_rut', right_on='nodo', how='left')

# Eliminar columnas redundantes
df_final = df_final.drop(['nodo'], axis=1)

# Agregar el directorio base
output_directory = '/home/cdsw/data/processed/'
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

# Sobreescribimos el dataframe fusionado en un archivo CSV en el directorio
df_final.to_csv(os.path.join(output_directory, 'contaminados_processed_iva_representante_total_with_labels.csv'), index=False)



