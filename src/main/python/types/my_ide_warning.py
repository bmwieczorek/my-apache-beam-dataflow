from typing import Optional


class MyClassWithWarning:
    """MyClassWithWarning sample.
    Args:
        my_param (Optional[str]):
            Random param.
    """

    def __init__(self, my_param=None):
        self.my_param = my_param


class MyClassOk:
    """MyClassOk sample.

        my_param: str, optional
            The recipient of the message
    """

    def __init__(self,
                 my_param=None  # type: Optional[str]
                 ):
        self.my_param = my_param


if __name__ == '__main__':
    # ide warning: Expected type '_SpecialForm[str]', got 'str' instead  if added: from typing import Optional
    # noinspection PyTypeChecker
    a = MyClassWithWarning("a")

    b = MyClassOk("b")

    # input("aa")  # built-in function
    # input = "aaa" # Shadows built-in name 'input'
