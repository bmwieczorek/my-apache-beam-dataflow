def print_conditionally(_flag):
    v = 1 if _flag else 0
    print(f"v={v}")


if __name__ == '__main__':
    print_conditionally(None)  # 0
    print_conditionally(False)  # 0
    print_conditionally("a")  # 1
    print_conditionally(True)  # 1
