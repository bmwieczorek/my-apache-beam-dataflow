from typing import List, Callable


class Processor:
    def process(self, elements: List[str]):
        raise NotImplemented


class NamedProcessor(Processor):
    def __init__(self, name):
        self.name = name

    def process(self, elements: List[str]):
        name = self.name if self.name else self.__class__.__name__
        print(f"Processing {len(elements)} element(s) by {name}")
        return elements

    def __rrshift__(self, name):
        self.name = name
        return self


class CallableProcessor(NamedProcessor):
    def __init__(self, fn: Callable[[str], str], name=None):
        super(CallableProcessor, self).__init__(name)
        self.fn = fn

    def process(self, elements: List[str]):
        elements2 = super().process(elements)
        return list(map(self.fn, elements2))


class ToLowerCase:
    def __call__(self, s: str):
        return s.lower()


def to_upper_case(s: str):
    return s.upper()


def main():
    upper_elements = CallableProcessor(to_upper_case).process(['a', 'b'])  # or CallableProcessor(lambda s: s.lower()))
    print(f"upper_elements={upper_elements}")
    lower__elements = ("To lower case processor" >> CallableProcessor(ToLowerCase())).process(['X', 'Y'])
    print(f"lower__elements={lower__elements}")


if __name__ == '__main__':
    main()
