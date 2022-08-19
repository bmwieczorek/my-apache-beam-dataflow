if __name__ == '__main__':
    try:
        raise Exception("My exception message")
    except Exception as e:
        e.args = ("additional info", *e.args)  # args unpacking
        raise
