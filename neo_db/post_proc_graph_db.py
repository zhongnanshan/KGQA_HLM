from config import graph
import json
from my_hash import my_hash

def test_get_nodes():
    ret = graph.run("MATCH (n:Person)-[r]->(m:Person) RETURN n,r,m")
    for record in ret:
        print("{0}-{1}->{2}".format(record['n']['Name'], record['r']['relation'], record['m']['Name']))

def test_graph_json():
    with open('./raw_data/relation1.json', encoding='utf-8-sig') as fd:
        json_data = json.load(fd)
        for item_dict in json_data:
            p1 = item_dict['p1']
            r = item_dict['r']
            p2 = item_dict['p2']
            print("{0}-{1}->{2}".format(p1['properties']['Name'], r['type'], p2['properties']['Name']))

# 从relation1.json生成relation2.json
def post_proc_graph_json():
    new_json_data = []
    with open('./raw_data/relation1.json', encoding='utf-8-sig') as fd:
        json_data = json.load(fd)
        for item_dict in json_data:
            p1 = item_dict['p1']
            r = item_dict['r']
            p2 = item_dict['p2']
            data_dict = {}
            data_dict['p1'] = {'labels': p1['labels'], 'properties': p1['properties']}
            data_dict['r'] = {'type': r['type'], 'properties': r['properties']}
            data_dict['p2'] = {'labels': p2['labels'], 'properties': p2['properties']}
            new_json_data.append(data_dict)

    with open('./raw_data/relation2.json', mode='w', encoding='utf-8') as fd:
        json.dump(new_json_data, fd, indent=True, ensure_ascii=False)

def get_all_nodes():
    nodes = graph.run("MATCH (n) RETURN n.Name")
    return nodes

def get_node_relation(node_name):
    relations = graph.run("""MATCH (n)-[r]->(m)
                            WHERE n.Name='{0}'
                            RETURN r,m.Name""".format(node_name))
    rel = []
    for relation in relations:
        rel_data = {'relation': relation['r'], 'node': relation['m.Name']}
        rel.append(rel_data)
    return rel

def get_node(node_name):
    node = graph.run("""MATCH (n) 
                        WHERE n.Name='{0}' 
                        RETURN labels(n) AS labels, n AS properties""".format(node_name))
    data = node.next()
    dt = {'labels': data['labels'], 'properties': data['properties']}
    dt_hash = my_hash(dt)
    dt['properties']['ID'] = dt_hash
    return dt

# 同步关系目标节点的ID号(如果有则不修改。尽在初次生成Neo4j导入数据时使用)
def update_id(json_data):
    id_dict = {}
    for item in json_data:
        id_dict[item['node']['properties']['Name']] = item['node']['properties']['ID']

    new_json_data = []
    for item in json_data:
        data = {'node': item['node']}
        data['relations'] = []
        for relation in item['relations']:
            old_node = relation['node']
            new_node = {'Name': old_node, 'ID': id_dict[old_node]}
            new_relation = {'relation': relation['relation'], 'node': new_node}
            data['relations'].append(new_relation)
        new_json_data.append(data)

    return new_json_data

# 从Neo4j数据库生成relation3.json
def post_proc_graph_db():
    nodes = get_all_nodes()
    json_data = []
    for record in nodes:
        node = get_node(record['n.Name'])
        relations = get_node_relation(record['n.Name'])
        data = {'node': node, 'relations': relations}
        json_data.append(data)
    
    new_json_data = update_id(json_data)

    with open('./raw_data/relation3.json', mode='w', encoding='utf-8') as fd:
        json.dump(new_json_data, fd, ensure_ascii=False, indent=1)

if __name__ == '__main__':
    # get_nodes()
    # test_graph_json()
    # post_proc_graph_json()
    post_proc_graph_db()
