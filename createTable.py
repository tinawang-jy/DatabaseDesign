import csv
import json
import os
import re
import shutil

class TableManager:
    def __init__(self, db_path, db_type):
        self.db_path = db_path
        self.db_type = db_type
        self.metadata_path = os.path.join(db_path, f"{os.path.basename(db_path)}_metadata.txt")
        self.metadata = self._read_metadata()

    def _read_metadata(self):
        # Read existing metadata or create new
        if os.path.exists(self.metadata_path):
            with open(self.metadata_path, 'r') as file:
                return json.load(file)
        else:
            return {'database_name': os.path.basename(self.db_path), 'tables': {}}

    def _write_metadata(self):
        # Write metadata to file
        with open(self.metadata_path, 'w') as file:
            json.dump(self.metadata, file, indent=4)

    def _parse_create_query(self, query):
        # Parse CREATE TABLE query
        pattern = r"CREATE TABLE (\w+) \((.+)\)"
        match = re.match(pattern, query)
        if not match:
            raise ValueError("Invalid CREATE TABLE query.")

        table_name, columns = match.groups()
        columns = columns.split(',')
        columns_info = {}
        for column in columns:
            col_parts = column.strip().split()
            col_name = col_parts[0]
            constraints = ' '.join(col_parts[1:])
            columns_info[col_name] = {'constraints': constraints}
        return table_name, columns_info

    def create_table(self, query):
        # Create a new table based on the query
        table_name, columns_info = self._parse_create_query(query)
        table_path = os.path.join(self.db_path, f"{table_name}.csv")

        if os.path.exists(table_path):
            raise FileExistsError(f"A table with the name '{table_name}' already exists.")

        with open(table_path, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=columns_info.keys())
            writer.writeheader()

        self.metadata['tables'][table_name] = columns_info
        self._write_metadata()

    def import_table(self, csv_path):
        # Import a table from CSV file
        table_name = os.path.basename(csv_path).split('.')[0]
        table_path = os.path.join(self.db_path, f"{table_name}.{self.db_type}")
        if self.db_type == 'csv':
            shutil.copyfile(csv_path, table_path)
        elif self.db_type == 'json':
            with open(csv_path, 'r') as file:
                reader = csv.DictReader(file)
                data = [row for row in reader]
            with open(table_path, 'w') as file:
                json.dump(data, file, indent=4)
        self.metadata['tables'][table_name] = {'imported': True}
        self._write_metadata()

# Example usage
if __name__ == "__main__":
    tm = TableManager("./Relational/test_db", "Relational")
    tm.create_table("CREATE TABLE employees (id{primary}, name, age{foreign reference department$dept_id})")
    print("Table 'employees' created.")
    tm.import_table("./basic.csv")
    print("Table 'employees' imported.")
