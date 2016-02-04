import luigi

class SimpleTask(luigi.Task):
    def output(self):
        return MockFile("SimpleTask", mirror_on_stderr=True)

    def run(self):
        _out = self.output().open('w')
        _out.write('Hello!')
        _out.close()

class DecoratedTask(luigi.Task):
    def output(self):
        return MockFile("DecoratedTask", mirror_on_stderr=True)

    def requires(self):
        return SimpleTask()

    def run(self):
        _in = self.input().open("r")
        _out = self.output().open('w')
        for line in _in:
            outval = "Decorated: " + line + '\n'
            _out.write(outval)

        _out.close()
        _in.close()

if __name__ == '__main__':
    from luigi.mock import MockFile
    luigi.run(["--local-scheduler"], main_task_cls=DecoratedTask)

