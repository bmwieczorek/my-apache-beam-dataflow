# python3 src/main/python/my_main.py
# Intellij File/Project Structure/Project Settings/Modules/Right click on maven module/Add/Python
# folder python right click Mark directory as Source Root

from typing import Dict, List, Optional

from my_lib import hello


def main():
    print("Hello World!")
    hello("bob")

    my_list = ['a', 'b']
    print(my_list)
    for e in my_list:
        print(e)
    for i in range(len(my_list)):
        print(my_list[i])
    my_transformed_list = list(map(lambda s: s.upper(), my_list))
    print(my_transformed_list)
    to_upper = lambda s: s.upper()
    my_transformed_list2 = list(map(to_upper, my_list))
    print(my_transformed_list2)

    my_dict = {'a': 1, 'b': 2}  # aka map
    print(my_dict)
    print(my_dict['a'])
    for k in my_dict:
        print(k, '->', my_dict[k])
    for k, v in my_dict.items():
        print(k, '->', v)


def process(name, version=None, *args, **kvargs):
    print(f"name={name}")
    print(f"version={version}")
    for e in args:
        print(f"arg={e}")
    for k, v in kvargs.items():
        print(k, '-->', v)


def process2(
        name,  # type: str
        version=None,  # type: Optional[str]
        args=None,  # type: Optional[List[str]]
        kvargs=None,  # type: Optional[Dict]
):
    print(f"name={name}")
    print(f"version={version}")
    for e in args:
        print(f"arg={e}")
    for k, v in kvargs.items():
        print(k, '-->', v)


def calc(*ints):
    result = 0
    for i in ints:
        result = result + i
    return result


def calc2(i: int):
    result = 0
    return result + i


if __name__ == "__main__":
    main()
    print("--------")
    process('aaa', 'x', 'y', 'z', a=1, b=2, c=3)
    print("++++++++")
    process(name='bbb')
    print("________")
    process('ccc', args=['X', 'Y', 'Z'], kvargs={"a": 1})
    print("---------")
    process2('ccc', args=['X', 'Y', 'Z'], kvargs={"a": 1})
    print(calc(1, 2, 3))
    #    print(calc('a', 'b'))
    print(calc2(2))
    # print(calc2('a'))  # TypeError: unsupported operand type(s) for +: 'int' and 'str'
