import networkx as nx
import copy
import time

# start time for script timer
start = time.time()

g = nx.read_shp("C:/Users/jacoken/PycharmProjects/NRN/data/ott_roads/ott_roads_test.shp")

print("Total number of graph nodes: " + str(nx.number_of_nodes(g)))
print("Total number of graph edges: " + str(nx.number_of_edges(g)))

g2 = g.to_undirected()

# print(g.degree())

# nx.set_node_attributes(g, deg, name='degree')

#nx.write_shp(g, "C:/Users/jacoken/PycharmProjects/NRN/data/ott_roads/ori.shp")

# g_other = copy.deepcopy(g)
# g_deadend = copy.deepcopy(g)

g_empty_other = nx.Graph()
g_empty_deadend = nx.Graph()
g_empty_two = nx.Graph()

other = [node for node, degree in g2.degree() if degree > 2]
dead_end = [node for node, degree in g2.degree() if degree == 0 or degree == 1]
two = [node for node, degree in g2.degree() if degree == 2]

g_empty_other.add_nodes_from(other)
g_empty_deadend.add_nodes_from(dead_end)
g_empty_two.add_nodes_from(two)

nx.write_shp(g_empty_other, "C:/Users/jacoken/PycharmProjects/NRN/data/ott_roads/results/other.shp")
nx.write_shp(g_empty_deadend, "C:/Users/jacoken/PycharmProjects/NRN/data/ott_roads/results/dead_end.shp")
nx.write_shp(g_empty_two, "C:/Users/jacoken/PycharmProjects/NRN/data/ott_roads/results/two.shp")

# output execution time
end = time.time()
hours, rem = divmod(end-start, 3600)
minutes, seconds = divmod(rem, 60)
print("{:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))