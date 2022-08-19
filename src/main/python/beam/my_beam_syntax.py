# python3 src/main/python/beam/my_main.py

from typing import Optional


class MyPTransform:
    def __init__(self, label=None):
        # type: (Optional[str]) -> None
        self.label = label

    def __rrshift__(self, label):
        return _MyNamedPTransform(self, label)

    def __str__(self):
        return f"{self.__class__.__name__}"

    def transform(self, input_p_collection):
        raise NotImplementedError("Call transform method from subclasses rather that from MyPTransform")


class _MyNamedPTransform(MyPTransform):
    def __init__(self, p_transform, label):
        super().__init__(label)
        self.p_transform = p_transform

    def transform(self, input_p_collection):
        return self.p_transform.transform(input_p_collection)

    def __str__(self):
        return f"{self.__class__.__name__} created with label {self.label}"


class MyCreate(MyPTransform):

    def __init__(self, values):
        super().__init__()
        self.values = values

    def transform(self, input_p_collection):
        p_collection = MyPCollection(self.values)
        print(f"{self} created {p_collection}")
        return p_collection

    def __str__(self):
        return f"MyCreate created with values {self.values}"


class MyMap(MyPTransform):

    def __init__(self, fn_callable):
        self.fn_callable = fn_callable
        super().__init__()

    def transform(self, p_collection):
        values = p_collection.values
        transformed_values = list(map(self.fn_callable, values))
        transformed_p_collection = MyPCollection(transformed_values)
        print(f"{self} transformed {p_collection} to {transformed_p_collection}")
        return transformed_p_collection

    def __str__(self):
        return f"MyMap created with callable {self.fn_callable}"


class MyTestPipeline:

    def __enter__(self):
        print("enter")
        self.pipeline = MyPipeline()
        return self.pipeline

    def __exit__(self, exc_type, exc_value, traceback):
        print("exit")
        self.pipeline.run()


class MyPipeline:

    # noinspection PyMethodMayBeStatic
    def apply(self, p_transform):
        print(p_transform)
        return MyPCollection(None)

    def __or__(self, p_transform):
        print(p_transform)
        transform = p_transform.transform(None)
        return transform

    # noinspection PyMethodMayBeStatic
    def run(self):
        print("run")


class MyPCollection:

    def __init__(self, values):
        self.values = values

    def apply(self, p_transform):
        print(p_transform)
        return self

    def __or__(self, p_transform):
        print(p_transform)
        transform = p_transform.transform(self)
        # return self
        return transform

    def __str__(self):
        return f"MyPCollection with {self.values}"


def main():
    p = MyPipeline()
    p | MyCreate(["a", "b", "a"]) | MyMap(lambda s: s.upper())
    p.run()

    with MyTestPipeline() as p:
        p | MyCreate(["a", "b", "a"]) | MyMap(lambda s: s.upper())

    with MyTestPipeline() as p:
        # p | 'create' >> MyCreate(["a", "b", "a"]) | 'map' >> MyMap([])
        words = (p | 'create' >> MyCreate(["a", "b", "a"]))
        upper = (words | 'map' >> MyMap(lambda s: s.upper()))

    print(f"words={words}")
    print(f"upper={upper}")


if __name__ == '__main__':
    main()
