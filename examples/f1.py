#!/usr/bin/env python

# http://ergast.com/api/f1/2004/1/results.json

from truflation.data.reader import ReaderRest
from truflation.data.task import Task

class F1Task(Task):
    def __init__(self):
        self.reader = ReaderRest('http://ergast.com/api')
    def run(self):
        b = self.reader.read_all('f1/2004/1/results.json')
        print(b.get())

if __name__ == '__main__':
    f1 = F1Task()
    f1.run()

