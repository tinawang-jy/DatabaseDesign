class Aggregator:
    def count(self, data):
        """Count the number of elements in the list."""
        count = 0
        for _ in data:
            count += 1
        return count

    def sum(self, data):
        """Calculate the sum of the elements, assuming they can be converted to numbers."""
        total = 0
        for item in data:
            total += float(item)
        return total

    def mean(self, data):
        """Calculate the mean of the elements."""
        total = 0
        count = 0
        for item in data:
            total += float(item)
            count += 1
        return total / count if count else 0

    def min(self, data):
        """Find the minimum value among the elements."""
        if not data:
            return 0
        min_value = float(data[0])
        for item in data:
            if float(item) < min_value:
                min_value = float(item)
        return min_value

    def max(self, data):
        """Find the maximum value among the elements."""
        if not data:
            return 0
        max_value = float(data[0])
        for item in data:
            if float(item) > max_value:
                max_value = float(item)
        return max_value