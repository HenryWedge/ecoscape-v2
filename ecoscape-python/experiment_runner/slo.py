class SLO:

    def __init__(self, query, threshold, is_bigger_better):
        self.query = query
        self.threshold = threshold
        self.is_bigger_better = is_bigger_better

    def get_name(self):
        return self.query.get_name()

    def get_value(self) -> float:
        return self.query.execute()

    def get_threshold(self):
        return self.threshold

    def is_uphold(self) -> bool:
        query_result = self.query.execute()
        if self.is_bigger_better:
            score = self.threshold - query_result
        else:
            score = query_result - self.threshold
        is_uphold = score < 1

        print(score)
        print(f"{query_result:.2f} ({self.query.get_name()}, {is_uphold})" )
        return is_uphold