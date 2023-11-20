import json
import csv

class Join:
    @staticmethod
    def merge_sort(arr, key):
        if len(arr) > 1:
            mid = len(arr) // 2
            L = arr[:mid]
            R = arr[mid:]

            Join.merge_sort(L, key)
            Join.merge_sort(R, key)

            i = j = k = 0

            while i < len(L) and j < len(R):
                if L[i][key] < R[j][key]:
                    arr[k] = L[i]
                    i += 1
                else:
                    arr[k] = R[j]
                    j += 1
                k += 1

            while i < len(L):
                arr[k] = L[i]
                i += 1
                k += 1

            while j < len(R):
                arr[k] = R[j]
                j += 1
                k += 1

    @staticmethod
    def inner_join(left_table, left_column, right_table, right_column):
        Join.merge_sort(left_table, left_column)
        Join.merge_sort(right_table, right_column)

        result = []
        i = j = 0
        while i < len(left_table) and j < len(right_table):
            if left_table[i][left_column] == right_table[j][right_column]:
                combined = {**left_table[i], **right_table[j]}
                result.append(combined)
                i += 1
                j += 1
            elif left_table[i][left_column] < right_table[j][right_column]:
                i += 1
            else:
                j += 1

        return result
    
    @staticmethod
    def left_join(left_table, left_column, right_table, right_column):
        Join.merge_sort(left_table, left_column)
        Join.merge_sort(right_table, right_column)

        result = []
        i = j = 0
        while i < len(left_table):
            matched = False
            while j < len(right_table) and right_table[j][right_column] <= left_table[i][left_column]:
                if left_table[i][left_column] == right_table[j][right_column]:
                    combined = {**left_table[i], **right_table[j]}
                    result.append(combined)
                    matched = True
                j += 1

            if not matched:
                result.append({**left_table[i], right_column: None})
            i += 1

        return result

    @staticmethod
    def right_join(left_table, left_column, right_table, right_column):
        return Join.left_join(right_table, right_column, left_table, left_column)

    @staticmethod
    def outer_join(left_table, left_column, right_table, right_column):
        left_joined = Join.left_join(left_table, left_column, right_table, right_column)
        right_joined = Join.right_join(left_table, left_column, right_table, right_column)

        result = left_joined
        right_joined_keys = {(item[left_column], item[right_column]) for item in right_joined if item[left_column] is not None}
        for item in right_joined:
            if (item[left_column], item[right_column]) not in right_joined_keys:
                result.append(item)

        return result
    
if __name__ == "__main__":
    def read_json_file(file_path):
        with open(file_path, 'r') as file:
            return json.load(file)
        
    def read_csv_file(file_path):
        with open(file_path, 'r') as file:
            reader = csv.reader(file)
            headers = next(reader, None)
            return [dict(zip(headers, row)) for row in reader]

    left_table = read_json_file("NoSQL/airbnb/basic/chunk_1.json")
    right_table = read_json_file("NoSQL/airbnb/host/chunk_2.json") 

    #left_table = read_csv_file("Relational/airbnb/basic/chunk_1.csv")
    #right_table = read_csv_file("Relational/airbnb/host/chunk_2.csv") 

    print("left_table:",len(left_table))  
    print("right_table",len(right_table)) 

    inner =  Join.inner_join(left_table,"host_id",right_table,"host_id")
    right = Join.right_join(left_table,"host_id",right_table,"host_id")
    left = Join.left_join(left_table,"host_id",right_table,"host_id")
    outer = Join.outer_join(left_table,"host_id",right_table,"host_id")

    print("inner:", len(inner))
    print("right:", len(right))
    print("left:", len(left))
    print("outer:", len(outer))
