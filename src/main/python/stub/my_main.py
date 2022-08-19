from example import stringify

if __name__ == '__main__':
    # stringify in example.py does not have arg/param type specified but has implementation
    # stringify in example.pyi (stub) defines input and return types but no implementation
    # IDE allows type validation: Expected type 'str', got 'int' instead
    # stringify(1)  # Expected type 'str', got 'int' instead

    # go to imp navigates to example.py, quick doc shows types defined in example.pyi stub
    s = stringify('a')
    print(f"s={s}")
