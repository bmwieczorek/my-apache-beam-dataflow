from typing import List


def transform_and_print(_list: List[str], s: slice):
    print(_list[s])


if __name__ == "__main__":
    slice_obj = slice(0, 2)
    transform_and_print(['a', 'b', 'c', 'd'], slice_obj)
    slice_obj = slice(-1)
    transform_and_print(['a', 'b', 'c', 'd'], slice_obj)
    print(['a', 'b', 'c', 'd'][-1])
    print(['a', 'b', 'c', 'd'][-2])
