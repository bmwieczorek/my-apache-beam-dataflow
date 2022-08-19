def my_args(*args):
    return args


def my_list(li):
    return li


def my_kwargs(**kwargs):
    return kwargs


def my_dict(d):
    return d


def my_arg_with_args(a, *args):
    return a, args


def my_arg_opt_arg_with_args(a, b=None, *args):
    return a, b, args


def my_args_kwargs(*args, **kwargs):
    return args, kwargs


def my_arg_opt_arg_with_args_kwargs(a, b=None, *args, **kwargs):
    return a, b, args, kwargs


def my_print(val):
    print(f"{val}, len={len(val)}")


if __name__ == '__main__':
    my_print(my_args())  # (), len=0
    my_print(my_args('x', 1, True))  # ('x', 1, True), len=3

    my_print(my_list([]))  # [], len=0 <-- difference
    my_print(my_list(list()))  # [], len=0 <-- difference
    my_print(my_list(['x', 1, True]))  # ['x', 1, True], len=3 <-- difference

    my_print(my_kwargs())  # {}, len=0
    my_print(my_kwargs(x='a', y=11, z=True))  # {'x': 'a', 'y': 11, 'z': True}, len=3

    my_print(my_dict(dict()))  # {}, len=0
    my_print(my_dict(dict(x='a', y=11, z=True)))  # {'x': 'a', 'y': 11, 'z': True}, len=3

    my_print(my_dict({}))  # {}, len=0
    my_print(my_dict({'x': 'a', 'y': 11, 'z': True}))  # {'x': 'a', 'y': 11, 'z': True}, len=3

    my_print(my_arg_with_args("a", "b", "c"))  # ('a', ('b', 'c')), len=2
    my_print(my_arg_opt_arg_with_args("a", "b", "c"))  # ('a', 'b', ('c',)), len=3
    my_print(my_args_kwargs('a', 'b', a=1, b=2))  # (('a', 'b'), {'a': 1, 'b': 2}), len=2
    my_print(my_arg_opt_arg_with_args_kwargs("a", "b", "c", x=1, y=2))  # ('a', 'b', ('c',), {'x': 1, 'y': 2}), len=4
