import json
from config import graph


def dict2str(dt):
    props = '{'
    for key in dt.keys():
        props += key + ': '
        props += '"' + dt[key] + '"' + ','
    props = props[:-1]
    props += '}'
    return props

def get_node_labels(node):
    labels = '|'.join(node['labels'])
    return labels

def get_node_props(node):
    props = dict2str(node['properties'])
    return props

def merge_node(labels, props):
    cmd = "MERGE (n:{0} {1})".format(labels, props)
    return cmd

def node_data_process(node):
    labels = get_node_labels(node)
    props = get_node_props(node)
    graph.run(merge_node(labels, props))

def merge_relation(p1_id, rel_type, rel_props, p2_id):
    cmd = """MATCH (n), (m)
            WHERE n.ID='{p1id}' AND m.ID='{p2id}'
            MERGE (n)-[r:{rt} {rp}]->(m)""".format(
                p1id=p1_id, rt=rel_type, rp=rel_props, p2id=p2_id)
    return cmd

def relations_data_process(node, relations):
    p1_id = node['properties']['ID']
    for relation in relations:
        rel_type = relation['relation']['relation']
        rel_props = dict2str(relation['relation'])
        p2_id = relation['node']['ID']
        graph.run(merge_relation(p1_id, rel_type, rel_props, p2_id))

def create_node():
    with open('./raw_data/relation3.json', encoding='utf-8') as fd:
        json_data = json.load(fd)
        for item in json_data:
            node_data = item['node']
            node_data_process(node_data)

def create_relations():
    with open('./raw_data/relation3.json', encoding='utf-8') as fd:
        json_data = json.load(fd)

        for item in json_data:
            node_data = item['node']
            relations_data = item['relations']
            relations_data_process(node_data, relations_data)


if __name__ == '__main__':
    create_node()
    create_relations()
