import os
import pandas as pd
import json
import csv
from join import Join
from aggregation import Aggregator

joiner = Join()
aggregator = Aggregator()

class Operator:
    @staticmethod
    def read_file(file_path):
        if file_path == 'metadata.json' or file_path.endswith('metadata.json'):
            return None, None

        if file_path.endswith('.json'):
            with open(file_path, 'r') as file:
                return json.load(file), 'json'

        elif file_path.endswith('.csv'):
            with open(file_path, 'r') as file:
                return [row for row in csv.DictReader(file)], 'csv'
            
        else:
            raise ValueError("Unsupported file format")
    
    def projection(self, table, column, db_path):
        table_path = os.path.join(db_path, table)

        # Iterate over each file in the table directory
        for file_name in os.listdir(table_path):
            file_path = os.path.join(table_path, file_name)
            data_iterator, file_type = Operator.read_file(file_path)

            if file_type == 'json' or file_type == 'csv':
                for record in data_iterator: 
                    selected_data = {col: record[col] for col in column if col in record}
                    yield selected_data

    def filtering(self,table, column, condition, db_path):
        table_path = os.path.join(db_path, table)

        # Define a function to apply the condition
        def apply_condition(row, column, method, value):
            if method == '=':
                return row[column] == value
            elif method == '!=':
                return row[column] != value
            elif method == '>':
                return float(row[column]) > float(value)
            elif method == '<':
                return float(row[column]) < float(value)
            elif method == '>=':
                return float(row[column]) >= float(value)
            elif method == '<=':
                return float(row[column]) <= float(value)
            else:
                raise ValueError("Invalid condition method")

        # Iterate over each file in the table directory
        for file_name in os.listdir(table_path):
            file_path = os.path.join(table_path, file_name)
            data_iterator, file_type = Operator.read_file(file_path)
            
            if file_type:
                for record in data_iterator:
                    if apply_condition(record, condition["variable"], condition["method"], condition["value"]):
                        yield {col: record[col] for col in column if col in record}

    def join(self, selected, left_table, left_column, right_table, right_column, join_method, db_path):
        def select_columns(joined_chunk, selected):
            for row in joined_chunk:
                selected_row = {}
                for sel in selected:
                    table_name = sel['table']
                    col_name = sel['column']
                    full_col_name = f"{table_name}_{col_name}" if table_name in row else col_name
                    # Check if the full column name exists in the row
                    if full_col_name in row:
                        selected_row[full_col_name] = row[full_col_name]
                    # If the column exists without the table prefix, use that value
                    elif col_name in row:
                        selected_row[col_name] = row[col_name]
                yield selected_row

        left_path = os.path.join(db_path, left_table)
        right_path = os.path.join(db_path, right_table)

        # Loop over each chunk file in the left table
        for left_file in os.listdir(left_path):
            left_data, file_type_left = Operator.read_file(os.path.join(left_path, left_file))

            # Loop over each chunk file in the right table
            for right_file in os.listdir(right_path):
                right_data, file_type_right = Operator.read_file(os.path.join(right_path, right_file))

                if file_type_left and file_type_right:
                    # Perform the join operation for each combination of left and right chunks
                    if join_method == "inner":
                        joined_chunk = joiner.inner_join(left_data, left_column, right_data, right_column)
                    elif join_method == "left":
                        joined_chunk = joiner.left_join(left_data, left_column, right_data, right_column)
                    elif join_method == "right":
                        joined_chunk = joiner.right_join(left_data, left_column, right_data, right_column)
                    elif join_method == "outer":
                        joined_chunk = joiner.outer_join(left_data, left_column, right_data, right_column)
                    else:
                        raise ValueError("Invalid join method")

                    # Yield selected columns from the joined chunk
                    yield from select_columns(joined_chunk, selected)   

    def group_agg(self, table, agg_column, agg_method, group_by_column, db_path):
        """ Aggregate data after grouping """
        table_path = os.path.join(db_path, table)

        aggregated_data = {}

        # Process each chunk file
        for file_name in os.listdir(table_path):
            data_iterator, file_type = Operator.read_file(os.path.join(table_path, file_name))

            if file_type: 
                # Group and aggregate data in the chunk
                for row in data_iterator:
                    key = row[group_by_column]
                    if key not in aggregated_data:
                        aggregated_data[key] = []

                    # Convert the string to a float for aggregation
                    try:
                        value = float(row[agg_column])
                    except ValueError:
                        # Handle cases where conversion to float fails
                        continue  # or use 'value = 0' if that makes sense in your context

                    aggregated_data[key].append(value)

        # Final aggregation
        for key, data in aggregated_data.items():  
            if agg_method == 'COUNT':
                aggregated_data[key] = aggregator.count(data)
            elif agg_method == 'SUM':
                aggregated_data[key] = aggregator.sum(data)
            elif agg_method == 'MEAN':
                aggregated_data[key] = aggregator.mean(data)
            elif agg_method == 'MIN':
                aggregated_data[key] = aggregator.min(data)
            elif agg_method == 'MAX':
                aggregated_data[key] = aggregator.max(data)
            else:
                raise ValueError("Invalid aggregation method")

        return aggregated_data

    def ordering(self, table, order_by_column, order_method, db_path):
        def merge_sort(data, key):
            if len(data) <= 1:
                return data

            mid = len(data) // 2
            left = merge_sort(data[:mid], key)
            right = merge_sort(data[mid:], key)

            return merge(left, right, key)

        def merge(left, right, key):
            result = []
            i = j = 0

            while i < len(left) and j < len(right):
                left_val = convert(left[i][key])
                right_val = convert(right[j][key])

                if order_method == 'ASC':
                    if left_val <= right_val:
                        result.append(left[i])
                        i += 1
                    else:
                        result.append(right[j])
                        j += 1
                else:  # 'DESC'
                    if left_val >= right_val:
                        result.append(left[i])
                        i += 1
                    else:
                        result.append(right[j])
                        j += 1

            result.extend(left[i:])
            result.extend(right[j:])
            return result

        def convert(value):
            try:
                return float(value)
            except ValueError:
                return value

        table_path = os.path.join(db_path, table)

        sorted_chunks = []

        # Process and sort each chunk
        for file_name in os.listdir(table_path):
            file_path = os.path.join(table_path, file_name)
            data_iterator, file_type = Operator.read_file(file_path)
            if file_type:
                chunk_sorted = merge_sort(list(data_iterator), order_by_column)
                sorted_chunks.append(chunk_sorted)

        # Merge sorted chunks 
        while len(sorted_chunks) > 1:
            sorted_chunks[0] = merge(sorted_chunks[0], sorted_chunks.pop(1), order_by_column)

        # Flattening the list of sorted chunks
        sorted_data = [item for sublist in sorted_chunks for item in sublist]
        return sorted_data

if __name__ == "__main__":
    operator = Operator()

    db_path = 'airbnb_sample_1/'
    #database_directory = 'Relational/airbnb_sample/'

    proj_result = operator.projection("host", ["host_id", "host_name"], db_path)
    proj_result_list = list(proj_result)
    #print("Projection Result:", json.dumps(proj_result_list, indent=4))

    filter_condition = {"variable":"minimum_nights",
                        "method":">",
                        "value":"3"}
    filter_result = operator.filtering("basic", ['id', 'name'], filter_condition, db_path)
    filter_result_list = list(filter_result)
    #print("Filtering Result:", json.dumps(filter_result_list, indent=4))

    selected_columns = [{"table": "host", "column": "host_name"}, 
                        {"table": "basic", "column": "name"}]
    #join_result = operator.join(selected_columns, "host", "host_id", "basic", "host_id", "inner", db_path)
    #join_result_list = list(join_result)
    #print("Join Result:", json.dumps(join_result_list, indent=4))
    
    group_agg_result = operator.group_agg("basic", "minimum_nights", "SUM", "price", db_path)
    #print("Aggregation & Grouping Result:", json.dumps(group_agg_result, indent=4))

    order_result = operator.ordering("basic", "minimum_nights", "ASC", db_path)
    #print("Ordering Result", json.dumps(order_result, indent=4))