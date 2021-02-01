class CircularVersionNumber(object):

    def __init__(self, max_number: int, buffer_size: int,
                 allow_zero: bool = False):
        self.max_number: int = max_number
        self.buffer_size: int = buffer_size
        self.allow_zero: bool = allow_zero
        self._circular_bellow: int = self.max_number - self.buffer_size

    def next(self, current_no: int) -> int:
        if current_no < self.max_number:
            return current_no + 1
        else:
            if self.allow_zero:
                return 0
            else:
                return 1

    def gt(self, v1, v2) -> bool:
        if v1 == v2:
            return False
        result = v1 - v2
        if abs(result) >= self._circular_bellow:
            return 0 > result
        else:
            return 0 < result

    def lt(self, v1, v2) -> bool:
        if v1 == v2:
            return False
        return not self.gt(v1, v2)

    def ge(self, v1, v2) -> bool:
        if v1 == v2:
            return True
        return self.gt(v1, v2)

    def le(self, v1, v2) -> bool:
        if v1 == v2:
            return True
        return self.lt(v1, v2)


# tests
if __name__ == "__main__":
    c = CircularVersionNumber(1023, 23)
    tests = [
            [1023, 1, False],
            [1, 1023, True],
            [23, 22, True],
            [22, 23, False],
            [99, 99, False],
            [100, 99, True],
            [99, 100, False],
            [1023, 23, False],
            [1022, 24, True],
            ]
    for i in tests:
        assert c.gt(i[0], i[1]) == i[2]
    tests = [
            [1000, 1001],
            [1001, 1002],
            [1022, 1023],
            [1023, 1],
            [1024, 1],
            [1, 2],
            [2, 3],
            [22, 23],
            [23, 24],
            ]
    for i in tests:
        assert c.next(i[0]) == i[1]
