# from typing import Optional


class MyClass:
    """MyClass sample.

    Args:
        param (Optional[str]):
            Random param.
    """

    def __init__(self, param=None):
        self.param = param


if __name__ == '__main__':
    # ide warning: Expected type '_SpecialForm[str]', got 'str' instead  if added: from typing import Optional
    a = MyClass("a")
