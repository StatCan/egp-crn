import networkx as nx
import time

# start time for script timer
start = time.time()

g = nx.read_shp("C:/Users/jacoken/PycharmProjects/NRN/data/ott_roads/Road_Centrelines.shp")

print("Total number of graph nodes: " + str(nx.number_of_nodes(g)))
print("Total number of graph edges: " + str(nx.number_of_edges(g)))

result = []
for i in g.nodes():
    if g.degree(i) == 1 or g.degree(i) >= 3:
        print(g.degree(i))
        result.append(g.degree(i))

# output execution time
end = time.time()
hours, rem = divmod(end-start, 3600)
minutes, seconds = divmod(rem, 60)
print("{:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))