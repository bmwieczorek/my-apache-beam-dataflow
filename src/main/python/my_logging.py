
if __name__ == '__main__':
    import logging
    import sys
    logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(filename)s:%(lineno)d %(message)s')
    # or

    # file_handler = logging.FileHandler(filename='my_logging.py.log')
    # stdout_handler = logging.StreamHandler(stream=sys.stdout)
    # handlers = [file_handler, stdout_handler]
    # logging.basicConfig(level=logging.DEBUG, handlers=handlers,
    #                     format='%(asctime)s %(levelname)s %(filename)s:%(lineno)d %(message)s')
    logging.info("hello")
