def my_tuple(a, b):
    return a, b


if __name__ == '__main__':
    t = ('a', 1)
    print(f"t={t}")
    t2 = my_tuple('a', 1)
    print(f"t2={t2}")
    x, y = my_tuple('a', 1)
    print(f"x={x}, y={y}")
    emtpy_tuple = ()
    print(f"emtpy_tuple={emtpy_tuple}")
