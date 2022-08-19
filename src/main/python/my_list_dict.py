from typing import Callable, Any

if __name__ == '__main__':
    # list
    my_list = ['a', 'b']
    print(f"my_list={my_list}")

    for e in my_list:
        print(f"e={e}")
    for i in range(len(my_list)):
        print(f"my_list[{i}]={my_list[i]}")

    my_transformed_list = list(map(lambda s: s.upper(), my_list))
    print(f"my_transformed_list={my_transformed_list}")

    # possible but not recommended (use def instead)
    to_upper_lambda: Callable[[Any], Any] = lambda s: s.upper()

    my_transformed_list2 = list(map(to_upper_lambda, my_list))
    print(f"my_transformed_list2={my_transformed_list2}")

    my_list2 = ['a', 1]
    print(f"my_list2={my_list2}")

    def process(el):
        return f"{el}{el}"

    my_new_list = [f"{e}{e}" for e in my_list]
    my_new_list2 = [process(e) for e in my_list]  # same as above
    print(f"my_new_list={my_new_list}")  # my_new_list

    my_empty_list = list()
    print(f"my_empty_list={my_empty_list}")

    def to_upper_fn(s):
        return s.upper()

    my_transformed_list3 = list(map(to_upper_fn, my_list))
    print(f"my_transformed_list3={my_transformed_list3}")

    my_filtered_list = list(filter(lambda s: s == 'a', my_list))
    print(f"my_filtered_list={my_filtered_list}")

    # dict (aka map)
    my_dict = {'a': 1, 'b': "abc"}
    print(f"my_dict={my_dict}")
    print(f"my_dict['a']={my_dict['a']}")
    for k in my_dict:
        print(k, '->', my_dict[k])

    # noinspection PyTypeChecker
    for k, v in my_dict.items():
        print(k, '->', v)

    my_dict2 = {"boy": 1, "girl": 2, "men": 18, "woman": 17}
    print(f"my_dict2={my_dict2.items()}")
    my_sorted_dict2 = sorted(my_dict2.items(), key=lambda ele: ele[1])
    print(f"my_sorted_dict2={my_sorted_dict2}")

    my_any_type_list = ['a', 1]  # List[Any]
    my_empty_dict = dict()
    print(f"my_empty_dict={my_empty_dict}")
