from typing import Tuple, Optional


def calc_any(i):
    result = 0
    return result + i


def calc_int(i: int = 0) -> int:
    result = 0
    return result + i


def calc_int2(
        i=0,  # type: int
):
    result = 0
    return result + i


def create_tuple_str_int(_1: str, _2: int = None) -> Tuple[str, int]:
    return _1, _2


def create_tuple_str_int2(
        _1,  # type: str
        _2=None,  # type: Optional[int]
):
    return _1, _2


if __name__ == '__main__':
    calc_any(1)  # ok
    calc_int(1)  # ok
    calc_int2(1)  # ok

    print(calc_any('b'))  # no IDE warning as method signature def calc_any(i): allows arg of any types
    # runtime error: TypeError: unsupported operand type(s) for +: 'int' and 'str'

    # calc_int('a')  # IDE warning: Expected type 'int', got 'str' instead
    # runtime error: TypeError: unsupported operand type(s) for +: 'int' and 'str'

    # calc_int2('a') # IDE warning: Expected type 'int', got 'str' instead
    # runtime error: TypeError: unsupported operand type(s) for +: 'int' and 'str'

    create_tuple_str_int('a')
    create_tuple_str_int('a', 1)
    create_tuple_str_int2('a')
    create_tuple_str_int2('a', 1)
