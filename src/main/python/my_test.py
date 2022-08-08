# add requirements.txt in main project folder and optionally restart Intellij
# python3 -m pytest src/main/python -o log_cli=true -v --junit-xml=target/TEST-results.xml

import unittest

# pip install pytest


class MyTest(unittest.TestCase):

    def test(self):
        a = 1
        self.assertEqual(1, a)


if __name__ == '__main__':
    unittest.main()
