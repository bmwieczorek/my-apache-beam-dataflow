# python3 src/main/python/my_main.py
# Intellij File/Project Structure/Project Settings/Modules/Right click on maven module/Add/Python
# folder python right click Mark directory as Source Root

from typing import Optional


class MyPTransform:
    def __init__(self, label=None):
        # type: (Optional[str]) -> None
        self.label = label

    def __rrshift__(self, label):
        return _MyNamedPTransform(self, label)

    def __str__(self):
        return f"{self.__class__.__name__}"

    def transform(self, input):
        return None


class _MyNamedPTransform(MyPTransform):

    def __init__(self, ptransform, label):
        super().__init__(label)
        self.ptransform = ptransform

    def transform(self, input):
        return self.ptransform.transform(input)

    def __str__(self):
        return f"{self.__class__.__name__} created with label {self.label}"


class MyCreate(MyPTransform):

    def __init__(self, values):
        super().__init__()
        self.values = values

    def transform(self, input):
        p_collection = MyPCollection(self.values)
        print(f"{self} created {p_collection}")
        return p_collection

    def __str__(self):
        return f"MyCreate created with values {self.values}"


class MyMap(MyPTransform):

    def __init__(self, callable):
        self.callable = callable
        super().__init__()

    def transform(self, p_collection):
        values = p_collection.values
        transformed_values = list(map(self.callable, values))
        transformed_p_collection = MyPCollection(transformed_values)
        print(f"{self} transformed {p_collection} to {transformed_p_collection}")
        return transformed_p_collection

    def __str__(self):
        return f"MyMap created with callable {self.callable}"


class MyTestPipeline:

    def __enter__(self):
        print("enter")
        self.pipeline = MyPipeline()
        return self.pipeline

    def __exit__(self, exc_type, exc_value, traceback):
        print("exit")
        self.pipeline.run()


class MyPipeline:

    def apply(self, ptransform):
        print(ptransform)
        return MyPCollection()

    def __or__(self, ptransform):
        print(ptransform)
        transform = ptransform.transform(None)
        return transform

    def run(self):
        print("run")


class MyPCollection:
    def __init__(self, values):
        self.values = values

    def apply(self, ptransform):
        print(ptransform)
        return self

    def __or__(self, ptransform):
        print(ptransform)
        transform = ptransform.transform(self)
        # return self
        return transform

    def __str__(self):
        return f"MyPCollection with {self.values}"


class MyCl:
    def __init__(self, name):
        self.name = name

    def __rshift__(self, other):
        print(f"shift {self.name} {other.name}")
        return other


def main():
    MyCl("abc1") >> MyCl("abc2") >> MyCl("abc3")

    # p = MyPipeline()
    # p | MyPTransform("create") | MyPTransform("map")
    # p.run()

    # with MyTestPipeline() as p:
    #     p | MyPTransform("create") | MyPTransform("map")

    with MyTestPipeline() as p:
        # p | 'create' >> MyCreate(["a", "b", "a"]) | 'map' >> MyMap([])
        words = (p | 'create' >> MyCreate(["a", "b", "a"]))
        upper = (words | 'map' >> MyMap(lambda s: s.upper()))

    print(f"words={words}")
    print(f"upper={upper}")


if __name__ == '__main__':
    main()
