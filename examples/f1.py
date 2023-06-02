#!/usr/bin/env python3

# http://ergast.com/api/f1/2004/1/results.json

from truflation.data.connector import ConnectorRest
from truflation.data.task import Task

class F1Task(Task):
    def __init__(self):
        self.reader = ConnectorRest('http://ergast.com/api')
    def run(self):
        b = self.reader.read_all('f1/2004/1/results.json')
        print(b)

if __name__ == '__main__':
    f1 = F1Task()
    f1.run()

