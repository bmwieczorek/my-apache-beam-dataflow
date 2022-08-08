# python3 src/main/python/my_main.py
# Intellij File/Project Structure/Project Settings/Modules/Right click on maven module/Add/Python
# folder python right click Mark directory as Source Root

from my_lib import hello


class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age

    def show(self):
        print(f"Person: {self.name}, {self.age}")


class Animal:

    def make_noise(self):
        print("unsupported")


class Dog(Animal):

    def make_noise(self):
        print("how how")


class PTrans:

    def __init__(self, name):
        self.name = name


class TestPipeline:

    def __enter__(self):
        print("enter")
        self.pipeline = Pipeline()
        return self.pipeline

    def __exit__(self, exc_type, exc_value, traceback):
        print("exit")
        self.pipeline.run()


class Pipeline:

    def apply(self, ptrans):
        print(ptrans.name)
        return PColl()

    def __or__(self, ptrans):
        print(ptrans.name)
        return PColl()

    def run(self):
        print("run")


class PColl:

    def apply(self, ptrans):
        print(ptrans.name)
        return self

    def __or__(self, ptrans):
        print(ptrans.name)
        return self


class MyCl:
    def __init__(self, name):
        self.name = name

    def __rshift__(self, other):
        print(f"shift {self.name} {other.name}")
        return other


def main():
    p1 = Person("bob", 12)
    # p1.show()
    p2 = Person(name="bob", age=10)
    # p2.show()

    my_dict = {"a": 1, "b": 2}
    print(my_dict["a"])

    # persons = {p1, p2}
    persons = [p1, p2, Person("foo", 13)]
    # for p in persons:
    for p in persons[:2]:
        p.show()

    a = Animal()
    a.make_noise()

    d = Dog()
    d.make_noise()
    d.my_iter("a", "b")
    d.my_iter2(bb=2, aa=1)

    x = {"boy": 1, "girl": 2, "men": 18, "woman": 17}
    print(x.items())
    print(sorted(x.items(), key=lambda y: y[1]))

    print("Hello World!")
    hello("bob")
    MyCl("abc1") >> MyCl("abc2") >> MyCl("abc3")
    # p = Pipeline()
    # p.apply(PTrans("create")).apply(PTrans("map"))
    # p.run()

    # p = Pipeline()
    # p | PTrans("create") | PTrans("map")
    # p.run()

    with TestPipeline() as pipeline:
        result = pipeline | PTrans("create") | PTrans("map")


if __name__ == '__main__':
    main()
