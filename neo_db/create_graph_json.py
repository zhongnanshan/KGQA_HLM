import json
from config import graph

def create_cypher(p1l, p1p, rt, rp, p2l, p2p):
    cmd = """MERGE (n:{p1l} {p1p})
            MERGE (m:{p2l} {p2p})
            MERGE (n)-[r:{rt} {rp}]->(m)""".format(
                p1l=p1l, p1p=p1p, rt=rt, rp=rp, p2l=p2l, p2p=p2p)
    return cmd

def dict2str(dt):
    props = '{'
    for key in dt.keys():
        props += key + ': '
        props += '"' + dt[key] + '"' + ','
    props = props[:-1]
    props += '}'
    return props

def get_p1l(item):
    p1_labels = item['p1']['labels']
    return '|'.join(p1_labels)

def get_p1p(item):
    p1_props = item['p1']['properties']
    return dict2str(p1_props)

def get_rt(item):
    r_type = item['r']['type']
    return r_type

def get_rp(item):
    r_props = item['r']['properties']
    return dict2str(r_props)

def get_p2l(item):
    p2_labels = item['p2']['labels']
    return '|'.join(p2_labels)

def get_p2p(item):
    p2_props = item['p2']['properties']
    return dict2str(p2_props)

def item_process(item):
    p1_labels = get_p1l(item)
    p1_props = get_p1p(item)
    r_type = get_rt(item)
    r_props = get_rp(item)
    p2_labels = get_p2l(item)
    p2_props = get_p2p(item)
    graph.run(create_cypher(p1_labels, p1_props, r_type, r_props, p2_labels, p2_props))

def main():
    with open('./raw_data/relation2.json', encoding='utf-8') as fd:
        json_data = json.load(fd)
        for item in json_data:
            item_process(item)

if __name__ == '__main__':
    main()
