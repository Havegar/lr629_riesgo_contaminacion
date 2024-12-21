import json
import pandas as pd
from py2neo import Graph

from neo4j import GraphDatabase
import matplotlib.pyplot as plt
import networkx as nx
from pyvis.network import Network

# Definir variables
uri = "bolt://10.33.1.189:4488"
user = "neo4j"
password = "Neo12345"


# Conexión a servidor local
graph = Graph(uri=uri, user=user, password=password)

#query = "DROP CONSTRAINT constraint_c3b81393"
#graph.run(query)

def LoadData_Nodes(_graph, file_nodos, sep="';'"):
    """
    Carga los datos de nodos desde un archivo CSV en Neo4j.
 
    Inputs:
    _graph: Objeto de la conexión a la base de datos Neo4j utilizando la librería pyneo.
    file_nodos (str): Ruta al archivo CSV que contiene los datos de nodos.
    sep (str): Separador de campos en el archivo CSV.
 
    Output:
    elapsed_t (float): Tiempo transcurrido en segundos durante la carga de datos.
    """
    import time
    start_t = time.process_time()
 
    # Crea un índice único en el atributo CONT_RUT de los nodos de tipo Contribuyente
    _graph.run("CREATE CONSTRAINT FOR (p:Contribuyente) REQUIRE p.CONT_RUT IS UNIQUE;")
    _graph.run("call db.awaitIndexes();")
 
    print("Cargando archivos de nodos: %s ..." % (file_nodos))
    # Carga los nodos desde el archivo CSV especificado
    load_nodes = _graph.run("""
        LOAD CSV WITH HEADERS FROM '%s' AS row
        CALL {
        WITH row
        CREATE (p:Contribuyente {
            cont_rut: row.cont_rut,
            score: coalesce(row.score, ''),
            total_pago_f29: coalesce(row.total_pago_f29, ''),
            IVA_neto: coalesce(row.IVA_neto, ''),
            unidad_regional: coalesce(row.unidad_regional, ''),
            n_documentos: coalesce(row.n_documentos, ''),
            lifetime: coalesce(row.lifetime, ''),
            alerta_inicial: coalesce(row.alerta_inicial, ''),
            comunidad: coalesce(row.comunidad, '')
            })
        RETURN row.cont_rut AS cont_rut
        } IN TRANSACTIONS OF 10000 rows
        RETURN *;
        """ % (file_nodos)).stats()
    print(load_nodes)
    elapsed_t = time.process_time() - start_t
    print("\nCarga de datos terminada en: %s " % (elapsed_t))
    return elapsed_t

LoadData_Nodes(graph, 'file:///contaminados_processed_iva_representante_total_with_labels.csv')

def LoadData_Arcs(graph, file_arcos, sep="';'"):
    """
    Carga los datos de arcos desde un archivo CSV en Neo4j.
 
    Inputs:
    _graph: Objeto de la conexión a la base de datos Neo4j utilizando la librería pyneo.
    file_arcos (str): Ruta al archivo CSV que contiene los datos de arcos.
    sep (str): Separador de campos en el archivo CSV.
 
    Output:
    elapsed_t (float): Tiempo transcurrido en segundos durante la carga de datos.
    """
    import time
    start_t = time.process_time()
 
    print("\nCargando archivo de relaciones: %s ..." % (file_arcos))
    # Carga los arcos desde el archivo CSV especificado
    load_rel = graph.run("""
    
        LOAD CSV WITH HEADERS FROM '%s' AS row
        CALL {
        WITH row
        MATCH (p1:Contribuyente {cont_rut:row.emisor})
        MATCH (p2:Contribuyente {cont_rut:row.receptor})
        MERGE (p1)-[rel:emite_a]->(p2) 
        SET
            rel.Fuerza = toFloat(row.Fi)
        RETURN count(rel) as count} IN TRANSACTIONS OF 10000 rows
        RETURN *;
        """ % (file_arcos)).stats()
    print(load_rel)
    elapsed_t = time.process_time() - start_t
    print("\nCarga de datos terminada en: %s " % (elapsed_t))
    return elapsed_t

LoadData_Arcs(graph, 'file:///fuerza_iva_representante.csv')

