class MyCallable:
    def __init__(self, prefix):
        self.prefix = prefix

    def __call__(self, name):
        return f"{self.prefix} {name}"


def hello(name=None):
    s = f"hello {name}!" if name else "hello!"
    print(s)
    return s


if __name__ == '__main__':
    my_callable = MyCallable('hello')
    print(my_callable('bob'))

    print(f"callable(my_callable)={callable(my_callable)}")
    print(f"callable(hello)={callable(hello)}")
    print(f"callable(lambda s: s)={callable(lambda s: s)}")
