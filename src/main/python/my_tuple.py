def my_tuple(a, b):
    return a, b


if __name__ == '__main__':
    t = my_tuple('a', 1)
    print(f"t={t}")
    x, y = my_tuple('a', 1)
    print(f"x={x}, y={y}")
