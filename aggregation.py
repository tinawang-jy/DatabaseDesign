class Aggregator:
    @staticmethod
    def count(self, data):
        count = 0
        for _ in data:
            count += 1
        return count
    
    @staticmethod
    def sum(self, data):
        total = 0
        for item in data:
            total += float(item)
        return total
    
    @staticmethod
    def mean(self, data):
        total = 0
        count = 0
        for item in data:
            total += float(item)
            count += 1
        return total / count if count else 0
    
    @staticmethod
    def min(self, data):
        if not data:
            return 0
        min_value = float(data[0])
        for item in data:
            if float(item) < min_value:
                min_value = float(item)
        return min_value
    
    @staticmethod
    def max(self, data):
        if not data:
            return 0
        max_value = float(data[0])
        for item in data:
            if float(item) > max_value:
                max_value = float(item)
        return max_value
    
if __name__ == "__main__":
    a = Aggregator()
    print(a.count([1,2,3,15]))
    print(a.sum([1,2,3,15]))
    print(a.mean([1,2,3,15]))
    print(a.min([1,2,3,15]))
    print(a.max([1,2,3,15]))
