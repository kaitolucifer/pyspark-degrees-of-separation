from enum import IntEnum, auto

from pyspark import SparkConf, SparkContext


class NodeColor(IntEnum):
    white = auto()
    gray = auto()
    black = auto()


conf = SparkConf().setMaster("local").setAppName("DegreesOfSeparation")
sc = SparkContext(conf=conf)

hit_counter = sc.accumulator(0)

start_character_id = 5306  # SpiderMan
target_character_id = 14  # ADAM 3,031

def convert_to_bfs(line):
    fields = line.split()
    hero_id = int(fields[0])
    connections = [int(c) for c in fields[1:]]
    color = NodeColor.white.value
    distance = 9999
    if (hero_id == start_character_id):
        color = NodeColor.gray.value
    distance = 0
    return (hero_id, (connections, distance, color))  # key, value


def create_starting_rdd():
    input_file = sc.textFile('Marvel-Graph.txt')
    return input_file.map(convert_to_bfs)


def bfs_map(node):
    character_id = node[0]  # key
    connections, distance, color = node[1]  # value
    results = []
    if (color == NodeColor.gray.value):
        for connection in connections:
            new_character_id = connection
            new_distance = distance + 1
            new_color = NodeColor.gray.value
            if (target_character_id == connection):
                hit_counter.add(1)

            new_entry = (new_character_id, ([], new_distance, new_color))
            results.append(new_entry)

        color = NodeColor.black.value
    results.append((character_id, (connections, distance, color)))
    return results


def bfs_reduce(data1, data2):
    edges1, distance1, color1 = data1
    edges2, distance2, color2  = data2

    distance = 9999
    color = NodeColor.white.value
    edges = []

    if len(edges1):
        edges = edges1
    elif len(edges2):
        edges = edges2
    
    distance = min(distance, distance1, distance2)
    color = max(color1, color2)  # darkest color

    return (edges, distance, color)


iteration_rdd = create_starting_rdd()
for i in range(1, 11):
    print(f"Running BFS iteration# {i}")
    mapped = iteration_rdd.flatMap(bfs_map)
    print(f"Processing {mapped.count()} values.")
    if hit_counter.value > 0:
        print(f"Hit the target character! From {hit_counter.value} different direction(s)")
        break
    iteration_rdd = mapped.reduceByKey(bfs_reduce)
