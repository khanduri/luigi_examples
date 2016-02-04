import datetime
import luigi


class SimpleTask(luigi.Task):

    date = luigi.DateParameter(default=datetime.date.today())
    comment = luigi.Parameter()
    path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget('data/'+"SimpleTask_%s" % self.comment)

    def run(self):
        with open(self.path, 'r') as _in:
            _out = self.output().open('w')
            for line in _in:
                _out.write(' ---- Hello! - %s \n' % line.strip())
            _out.close()


class WrappedTask(luigi.Task):

    date = luigi.DateParameter(default=datetime.date.today())
    comment = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget('data/'+"WrappedTask_%s" % self.comment)

    def _make_output(self, name):
        return luigi.LocalTarget('data/'+name)

    def run(self):
        _out = self.output().open('w')
        for idx in xrange(2):
            comment = 'wrap-%s-fan-%s' % (idx, self.comment)

            # NOTE: Given I want to use yield .. how bad is creating
            # the output here? What would happen if this runs multiple times?
            _out_file = self._make_output(comment)
            if _out_file.exists():
                continue
            _out = _out_file.open('w')
            outval = "  ==== Wrapper: %s \n" % comment.strip()
            _out.write(outval)
            _out.close()

            path = _out_file.path

            yield SimpleTask(date=self.date, comment=comment, path=path)
            _out.write(' ---- Done! - %s \n' % comment.strip())

        _out.close()

class FanoutTask(luigi.Task):

    def run(self):
        for idx in xrange(5):
            print ' ---- FANOUT - %s' % idx
            yield WrappedTask(comment=str(idx))


if __name__ == '__main__':
    from luigi.mock import MockFile
    luigi.run(["--local-scheduler"], main_task_cls=FanoutTask)

