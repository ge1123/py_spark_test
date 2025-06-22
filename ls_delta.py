import os

delta_root = "/tmp/delta"

for db in os.listdir(delta_root):
    db_path = os.path.join(delta_root, db)
    if not os.path.isdir(db_path): continue

    for schema in os.listdir(db_path):
        schema_path = os.path.join(db_path, schema)
        if not os.path.isdir(schema_path): continue

        for table in os.listdir(schema_path):
            print(f"{db}.{schema}.{table}")
