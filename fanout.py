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


class WrappedTask(luigi.Task):

    date = luigi.DateParameter(default=datetime.date.today())
    comment = luigi.Parameter()

    def output(self):
        return MockFile("WrappedTask_%s" % self.comment, mirror_on_stderr=True)

    def requires(self):
        return [
                SimpleTask(date=self.date, comment='one-%s' % self.comment),
                SimpleTask(date=self.date, comment='two-%s' % self.comment),
                ]

    def run(self):
        _out = self.output().open('w')
        _ins = self.input()

        for in_file in _ins:
            _in = in_file.open("r")
            for line in _in:
                outval = "  ==== Wrapper " + self.comment + ": " + line + '\n'
                _out.write(outval)
            _in.close()

        _out.close()


class FanoutTask(luigi.Task):

    def run(self):
        for idx in xrange(10):
            yield WrappedTask(comment=str(idx))


if __name__ == '__main__':
    from luigi.mock import MockFile
    luigi.run(["--local-scheduler"], main_task_cls=FanoutTask)

