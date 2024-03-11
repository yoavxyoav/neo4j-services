from pymongo import MongoClient
from neo4j import GraphDatabase

from creds import MONGO_URL, NEO4J_URL, NEO4J_TOKEN

client = MongoClient(MONGO_URL)
mongo_db = client['sansa']

neo4j_driver = GraphDatabase.driver(NEO4J_URL, auth=("neo4j", NEO4J_TOKEN))

# Convert ObjectId to string in the MongoDB data extraction part
services_data = list(mongo_db.services.find({}, {'_id': 1, 'name': 1, 'synonyms': 1, 'relatedServices': 1}))
medical_conditions_data = list(
    mongo_db.medicalconditions.find({}, {'_id': 1, 'name': 1, 'synonyms': 1, 'services.stage1': 1}))
medical_groups_data = list(mongo_db.medicalgroups.find({}, {'_id': 1, 'name': 1, 'synonyms': 1, 'services': 1}))

# Convert ObjectId to string
for data_list in [services_data, medical_conditions_data, medical_groups_data]:
    for item in data_list:
        item['_id'] = str(item['_id'])  # Convert ObjectId to string


def adjust_medical_conditions_data(data):
    for item in data:
        if 'services' in item and 'stage1' in item['services']:
            item['services'] = item['services']['stage1']


adjust_medical_conditions_data(medical_conditions_data)


def clear_neo4j_database(tx):
    tx.run("MATCH (n) DETACH DELETE n")


def create_nodes_in_batch(tx, nodes, label, exclude_fields=None):
    if exclude_fields is None:
        exclude_fields = []
    # Prepare nodes for insertion by removing excluded fields
    prepared_nodes = []
    for node in nodes:
        prepared_node = {k: v for k, v in node.items() if k not in exclude_fields}
        prepared_nodes.append(prepared_node)

    query = f"""
    UNWIND $nodes AS node
    MERGE (n:{label} {{_id: node._id}})
    ON CREATE SET n += node
    ON MATCH SET n += node
    """
    tx.run(query, nodes=prepared_nodes)


def create_relationships_in_batch(tx, relationships, rel_type):
    query = f"""
    UNWIND $relationships AS rel
    MATCH (a {{_id: rel._id1}}), (b {{_id: rel._id2}})
    MERGE (a)-[r:{rel_type}]->(b)
    """
    tx.run(query, relationships=relationships)


def batch_process(session, data, batch_size, process_function, *args):
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        session.execute_write(process_function, batch, *args)


# Helper function to extract relationship data
def prepare_relationship_data(source_data, rel_field, rel_type):
    relationships = []
    for item in source_data:
        if rel_field in item:
            for related_id in item[rel_field]:
                # Convert both the item's _id and the related_id to string
                relationships.append({
                    '_id1': str(item['_id']),
                    '_id2': str(related_id),  # Ensure this conversion if related_id might be ObjectId
                    'type': rel_type
                })
    return relationships


batch_size = 100
with neo4j_driver.session() as session:
    session.execute_write(clear_neo4j_database)

    service_exclude_fields = ['relatedServices']
    medical_conditions_exclude_fields = ['services']
    medical_groups_exclude_fields = ['services']

    batch_process(session, services_data, batch_size, create_nodes_in_batch, 'Service', service_exclude_fields)
    batch_process(session, medical_conditions_data, batch_size, create_nodes_in_batch, 'MedicalCondition',
                  medical_conditions_exclude_fields)
    batch_process(session, medical_groups_data, batch_size, create_nodes_in_batch, 'MedicalGroup',
                  medical_groups_exclude_fields)

    related_services_relationships = prepare_relationship_data(services_data, 'relatedServices', 'RELATED_TO')
    condition_service_relationships = prepare_relationship_data(medical_conditions_data, 'services', 'CONTAINS')
    group_service_relationships = prepare_relationship_data(medical_groups_data, 'services', 'CONTAINS')

    batch_process(session, related_services_relationships, batch_size, create_relationships_in_batch, 'RELATED_TO')
    batch_process(session, condition_service_relationships, batch_size, create_relationships_in_batch, 'CONTAINS')
    batch_process(session, group_service_relationships, batch_size, create_relationships_in_batch, 'CONTAINS')

neo4j_driver.close()
