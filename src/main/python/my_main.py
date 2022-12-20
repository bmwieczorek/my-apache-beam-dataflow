# python3 src/main/python/my_main.py
# Intellij File/Project Structure/Project Settings/Modules/Right click on maven module/Add/Python
# folder python right click Mark directory as Source Root

from my_lib import hello
import my_package as my
from my_package import my_add
from my_package.extra import my_multiply

# python pip package name contains '-' instead of '_": pip install my-python-package
# no warning in yellow as my_python_package folder name matches pip package: my-python-package (except _ -> -)


def main():
    print("Hello World!")
    hello("bob")

    # import via alias
    print(my.my_add(1, 2))
    print(my.extra.my_multiply(1, 2))

    # import methods
    print(my_add(1, 2))
    print(my_multiply(1, 2))


if __name__ == "__main__":
    main()
