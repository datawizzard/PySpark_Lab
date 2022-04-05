from azure.data.tables import TableServiceClient
from azure.data.tables import TableEntity
from azure.core.credentials import AzureNamedKeyCredential
credential = AzureNamedKeyCredential("divya21", "q1tJhy+iTv2Di+jwB9Cy0+UkVjLwwvdjjhl14OytZdDcYVagQQlFqukwNjZsLi9TiihJ2wPMO8Hm+AStOCbMcg==")
table_service = TableServiceClient(endpoint="https://divya21.table.core.windows.net/", credential=credential)
table_service = TableServiceClient.from_connection_string(conn_str='DefaultEndpointsProtocol=https;AccountName=divya21;AccountKey=hlHjuL4Njndo1L8wd7pX6cBC9AuZmg8JrbXHK8D2OXZZLutCKalUBwZuqRLQyYsfTLvYQhfHD9iY+AStVAxgRg==;EndpointSuffix=core.windows.net')
# table_service.create_table('tasktable')
table_client = table_service.get_table_client(table_name="tasktable")
task = {u'PartitionKey': u'tasksSeattle', u'RowKey': u'001',
        u'description': u'Take out the trash', u'priority': 200}
# table_client.create_entity(entity=task)
task = TableEntity()
task[u'PartitionKey'] = u'tasksSeattle'
task[u'RowKey'] = u'002'
task[u'description'] = u'Wash the car'
task[u'priority'] = 100
# table_client.create_entity(task)
task = {u'PartitionKey': u'tasksSeattle', u'RowKey': u'001',
        u'description': u'Take out the garbage', u'priority': 250}
# table_client.update_entity(task)
# Replace the entity created earlier
task = {u'PartitionKey': u'tasksSeattle', u'RowKey': u'001',
        u'description': u'Take out the garbage again', u'priority': 250}
# table_client.upsert_entity(task)

# Insert a new entity
task = {u'PartitionKey': u'tasksSeattle', u'RowKey': u'003',
        u'description': u'Buy detergent', u'priority': 300}
# table_client.upsert_entity(task)
# print(task)
task004 = {u'PartitionKey': u'tasksSeattle', u'RowKey': '004',
           'description': u'Go grocery shopping', u'priority': 400}
task005 = {u'PartitionKey': u'tasksSeattle', u'RowKey': '005',
           u'description': u'Clean the bathroom', u'priority': 100}
operations = [("create", task004), ("create", task005)]
# table_client.submit_transaction(operations)

task = table_client.get_entity('tasksSeattle', '001')
print(task['description'])
print(task['priority'])

tasks = table_client.query_entities(query_filter="PartitionKey eq 'tasksSeattle'")
for task in tasks:
    print(task['description'])
    print(task['priority'])

tasks = table_client.query_entities(
    query_filter="PartitionKey eq 'tasksSeattle'", select='description')
for task in tasks:
    print(task['description'])

print("Get the first item from the table")
tasks = table_client.list_entities()
lst = list(tasks)
print(lst[0])