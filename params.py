import datetime
import luigi


class SimpleTask(luigi.Task):

    date = luigi.DateParameter(default=datetime.date.today())
    comment = luigi.Parameter()

    def output(self):
        return MockFile("SimpleTask_%s" % self.comment, mirror_on_stderr=True)

    def run(self):
        _out = self.output().open('w')
        _out.write(' ---- Hello! - %s \n' % self.comment)
        _out.close()


SimpleTask1 = SimpleTask
SimpleTask2 = SimpleTask


class DecoratedTask(luigi.Task):

    date = luigi.DateParameter(default=datetime.date.today())

    def output(self):
        return MockFile("DecoratedTask", mirror_on_stderr=True)

    def requires(self):
        return [
                SimpleTask1(date=self.date, comment='one'),
                SimpleTask2(date=self.date, comment='two'),
                ]

    def run(self):
        _out = self.output().open('w')
        _ins = self.input()

        for in_file in _ins:
            _in = in_file.open("r")
            for line in _in:
                outval = "  ==== Decorated: " + line + '\n'
                _out.write(outval)
            _in.close()

        _out.close()


if __name__ == '__main__':
    from luigi.mock import MockFile
    luigi.run(["--local-scheduler"], main_task_cls=DecoratedTask)

