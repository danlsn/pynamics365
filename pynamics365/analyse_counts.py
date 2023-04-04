import json
import pandas as pd
import client

environment_json = json.loads(open('.environment.json').read())

dc = client.DynamicsClient()
entities = dc.entities

counts = environment_json['https://org30a87.crm5.dynamics.com/api/data/v9.2']['record_counts']
entity_counts = []
for entity, count in counts.items():
    try:
        display_name = entities[entity]['DisplayName']['UserLocalizedLabel']['Label']
        description = entities[entity]['Description']['UserLocalizedLabel']['Label'] or ''
    except TypeError:
        display_name = entity
        description = ''
    logical_name = entities[entity]['LogicalName']
    entity_counts.append((entity, logical_name, display_name, description, count['record_count']))

entity_counts.sort(key=lambda x: x[1], reverse=True)
df = pd.DataFrame(entity_counts, columns=['Entity', 'LogicalName', 'Name', 'Description', 'Count'])
# Sort by count
df = df.sort_values(by='Count', ascending=False)
df.to_csv('entity_counts.csv', index=False)
...
