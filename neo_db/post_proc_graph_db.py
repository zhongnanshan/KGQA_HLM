from config import graph
import json

def get_nodes():
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


if __name__ == '__main__':
    # get_nodes()
    # test_graph_json()
    post_proc_graph_json()
