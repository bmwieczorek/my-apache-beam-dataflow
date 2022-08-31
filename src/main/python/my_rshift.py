class MyCl:
    def __init__(self, name):
        self.name = name
        self.elements = [name]

    def __rshift__(self, other):
        print(f"shift {self.name} -> {other.name}, {self.elements}")
        for e in self.elements:
            other.elements.append(e)
        return other


def main():
    MyCl("abc1") >> MyCl("abc2") >> MyCl("abc3") >> MyCl("abc4")


if __name__ == '__main__':
    main()
