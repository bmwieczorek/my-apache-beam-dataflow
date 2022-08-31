# python3 src/main/python/my_main.py
# Intellij File/Project Structure/Project Settings/Modules/Right click on maven module/Add/Python
# folder python right click Mark directory as Source Root

from my_lib import hello


def main():
    print("Hello World!")
    hello("bob")


if __name__ == "__main__":
    main()
