
from neo4j import GraphDatabase

uri = "bolt://localhost:7687"
userName = "neo4j"
password = "mantas2002"


graphDB_Driver = GraphDatabase.driver(uri, auth=(userName, password))


def riverByName():




    cqlFindRiverByName = "MATCH (r:river)WHERE r.name = 'River C' RETURN r.name"

    with graphDB_Driver.session() as graphDB_Session:
        result = graphDB_Session.run(cqlFindRiverByName)
        for record in result:
            riverName = record["r.name"]
            print(riverName)
     


        

def riverflows():
    allRiverFLows = "River B"
    cqlQuery = (
        "MATCH (r:river)-[:flows_to]->(x:river) WHERE r.name = 'River B' RETURN x.name"
    )

    with graphDB_Driver.session() as graphDB_Session:
        result = graphDB_Session.run(cqlQuery)

        for record in result:

            riverName = record["x.name"]
            print(riverName)


def findTotalFlow():
    cqlPath = """
    MATCH paths = (n1)-[*]->(n2) WHERE n1.name = 'River A' AND n2.name = 'River D' RETURN paths,  REDUCE(s = 0, rel IN relationships(paths) | s + rel.flowRate) AS totalFlow
    """

    with graphDB_Driver.session() as graphDB_Session:
        result = graphDB_Session.run(cqlPath)

        for record in result:
            print(f"flow: {record['totalFlow']}")
            nodes = record["paths"].nodes
            for node in nodes:
                print(node["name"])


def findPaths():
    cqlPath = """
    MATCH paths = (n1)-[*]->(n2) WHERE n1.name = 'Source' AND n2.name = 'River H' RETURN paths
    """
    result = graphDB_Session.run(cqlPath)

    # Iterate over the result set and print each path
    for record in result:
        print("\n")
        nodes = record["paths"].nodes
        for node in nodes:
            print(node["name"])


def findShortestPaths():
    cqlShorestPath = """MATCH (p1:river { name: 'Source' }),(p2:river { name: 'River H' }), path = shortestPath((p1)-[*..15]-(p2)) RETURN path"""

    cqlShorestPaths = """MATCH (p1:river { name: 'Source' }),(p2:river { name: 'River H' }), path = allShortestPaths((p1)-[*..15]->(p2)) RETURN path"""



    shortestPath = graphDB_Session.run(cqlShorestPath)

    print("Shortest path between nodes :")

    for record in shortestPath:
        nodes = record["path"].nodes
        for node in nodes:
            print(node["name"])


    shortestPaths = graphDB_Session.run(cqlShorestPaths)

    print("======")

    print("Shortest paths between nodes :")

    pathCount = 0

    for record in shortestPaths:
        pathCount = pathCount + 1
        nodes = record["path"].nodes
        print("Path %d:" % (pathCount))
        for node in nodes:
            print(node["name"])


def findShortestPathsByFlow():
    cqlPath = """
    MATCH paths = (n1)-[*]->(n2) 
    WHERE n1.name = 'River B' AND n2.name = 'River H' 
    RETURN paths, REDUCE(s = 0, rel IN relationships(paths) | s + rel.flowRate) AS totalFlow
    ORDER BY totalFlow
    LIMIT 1
    """

    with graphDB_Driver.session() as graphDB_Session:
        result = graphDB_Session.run(cqlPath)

        for record in result:
            print(f"Shortest path: {record['totalFlow']}")
            nodes = record["paths"].nodes
            for node in nodes:
                print(node["name"])



cqlCreate = """
CREATE (source:river { name: "Source"}),
       (riverA:river { name: "River A"}),
       (riverB:river { name: "River B"}),
       (riverC:river { name: "River C"}),
       (riverD:river { name: "River D"}),
       (riverE:river { name: "River E"}),
       (riverF:river { name: "River F"}),
       (riverG:river { name: "River G"}),
       (riverH:river { name: "River H"})

CREATE (source)-[:flows_to {flowRate: 50}]->(riverA)
CREATE (source)-[:flows_to {flowRate: 30}]->(riverB)
CREATE (riverA)-[:flows_to {flowRate: 20}]->(riverC)
CREATE (riverB)-[:flows_to {flowRate: 25}]->(riverC)
CREATE (riverB)-[:flows_to {flowRate: 50}]->(riverD)
CREATE (riverB)-[:flows_to {flowRate: 30}]->(riverF)
CREATE (riverA)-[:flows_to {flowRate: 20}]->(riverG)

CREATE (riverG)-[:flows_to {flowRate: 20}]->(riverD)
CREATE (riverC)-[:flows_to {flowRate: 25}]->(riverD)
CREATE (riverD)-[:flows_to {flowRate: 25}]->(riverE)
CREATE (riverE)-[:flows_to {flowRate: 25}]->(riverH)
CREATE (riverF)-[:flows_to {flowRate: 25}]->(riverH)
"""


cqlDelete = "MATCH (n:river {name: 'River E'}) DETACH DELETE n"



with graphDB_Driver.session() as graphDB_Session:


    # CQL to delete all nodes and relationships
    """cqlDeleteAll = "MATCH (n) DETACH DELETE n"

    # Execute the CQL query to delete all nodes and relationships
    with graphDB_Driver.session() as graphDB_Session:
        graphDB_Session.run(cqlDeleteAll)
        print("All nodes and relationships have been deleted.")"""

    # graphDB_Session.run(cqlCreate)
    # graphDB_Session.run(cqlDelete)

    riverByName()
    #findPaths()
    #riverflows()
    #findShortestPathsByFlow()
    #findShortestPaths()
    #findTotalFlow()

