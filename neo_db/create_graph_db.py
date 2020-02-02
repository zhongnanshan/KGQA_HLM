import re
from config import graph

comment_pat = re.compile(r'^\s*#+.*$')
empty_pat = re.compile(r"^\s*$")

# 判断是否注释行
def is_comment_line(line):
    return comment_pat.match(line) != None

# 判断是否空行
def is_empty_line(line):
    return empty_pat.match(line) != None

# 判断是否关系行
def is_relation_line(line):
    datas = re.split(r";", line.strip())
    return datas[-1].lower() == 'r'

# 判断是否属性行
def is_property_line(line):
    datas = re.split(r";", line.strip())
    return datas[-1].lower() == 'p'

# 提取关系属性并返回字典列表
def relation_property_process(datas):
    property_dict = {}
    for data in datas:
        dt = re.split(r",", data.strip())
        if dt[0] == '排行':
            property_dict['num'] = dt[1]
        elif dt[0] == '封号':
            property_dict['title'] = dt[1]
        elif dt[0] == '出自':
            property_dict['from'] = dt[1]
        else:
            raise ValueError('不能识别的关系属性!')
    return property_dict

# 生成MERGE cypher指令
def merge_node(node_name):
    cmd = "MERGE(p: Person {{Name: '{0}'}})".format(node_name)
    return cmd

# 生成CREATE RELATION cypher指令
def create_relation(p1, p2, rel):
    cmd = """MATCH (e: Person),(cc: Person)
        WHERE e.Name='{0}' AND cc.Name='{1}'
        CREATE (e)-[r:{2} {{relation: '{2}'}}]->(cc)""".format(p1, p2, rel)
    return cmd

# 生成RELATION检测cypher指令
def create_relation_check(p1, p2, rel):
    cmd = """MATCH (e: Person)-[r:{2}]->(cc: Person)
        WHERE e.Name='{0}' AND cc.Name='{1}'
        RETURN count(r)""".format(p1, p2, rel)
    return cmd

# 生成SET RELATION PROPERTY cypher指令
def set_relation_property(p1, p2, rel, key, val):
    cmd = """MATCH (e: Person)-[r:{2}]->(cc:Person)
        WHERE e.Name='{0}' AND cc.Name='{1}'
        SET r.{3}='{4}'""".format(p1, p2, rel, key, val)
    return cmd

# 生成SET NODE PEOPERTY cypher指令
def set_node_property(node_name, key, val):
    cmd = """MATCH (n: Person)
        WHERE n.Name='{0}'
        SET n.{1}='{2}'""".format(node_name, key, val)
    return cmd

# 检测数据中是否已存在关系
def is_exist_relation(p1, p2, rel):
    ret = graph.run(create_relation_check(p1, p2, rel))
    return ret.evaluate() > 0

# 处理关系行
def realtion_process(line):
    datas = re.split(r";", line.strip())
    relation = datas[0]
    rel_property = relation_property_process(datas[1:-1])
    rel_data = re.split(r",", relation.strip())
    p1 = rel_data[0].strip()
    p2 = rel_data[1].strip()
    rel = rel_data[2].strip()
    graph.run(merge_node(p1))
    graph.run(merge_node(p2))
    if not is_exist_relation(p1, p2, rel):
        graph.run(create_relation(p1, p2, rel))
    for key in rel_property.keys():
        graph.run(set_relation_property(p1, p2, rel, key, rel_property[key]))

# 处理节点属性
def node_property_key_process(datas):
    data = ''
    if datas[1] == '住地':
        data = 'address', datas[2]
    elif datas[1] == '又名':
        data = 'alias', datas[2]
    elif datas[1] == '出场':
        data = 'chapter', datas[2]
    elif datas[1] == '家族':
        data = 'family', datas[2]
    elif datas[1] == '爱好':
        data = 'hobby', datas[2]
    elif datas[1] == '学历':
        data = 'education', datas[2]
    elif datas[1] == '字':
        data = 'word', datas[2]
    elif datas[1] == '职位':
        data = 'official', datas[2]
    elif datas[1] == '身份':
        data = 'status', datas[2]
    elif datas[1] == '名':
        data = 'name', datas[2]
    elif datas[1] == '别号':
        data = 'number', datas[2]
    elif datas[1] == '爵位':
        data = 'rank', datas[2]
    elif datas[1] == '出身':
        data = 'origin', datas[2]
    elif datas[1] == '世袭':
        data = 'inherit', datas[2]
    elif datas[1] == '擅长':
        data = 'goodat', datas[2]
    elif datas[1] == '绰号':
        data = 'nickname', datas[2]
    elif datas[1] == '归类':
        data = 'class', datas[2]
    else:
        raise ValueError('不能识别节点属性!')
    return data

# 处理属性行
def node_property_process(line):
    datas = re.split(r";", line.strip())
    prop_datas = re.split(r",", datas[0].strip())
    props = node_property_key_process(prop_datas)
    graph.run(merge_node(prop_datas[0].strip()))
    graph.run(set_node_property(prop_datas[0].strip(), props[0], props[1]))

# 单行处理函数
def line_process(line):
    if is_relation_line(line):
        realtion_process(line)
    elif is_property_line(line):
        node_property_process(line)
    else:
        raise ValueError('不能识别输入行是关系还是属性!')

# 脚本主入口
if __name__ == '__main__':
    with open('./raw_data/relation1.txt', encoding='utf-8') as fd:
        lines = fd.readlines()
        for line in lines:
            if not (is_comment_line(line) or is_empty_line(line)):
                line_process(line)
