import luigi

class SimpleTask(luigi.Task):
    def output(self):
        return MockFile("SimpleTask", mirror_on_stderr=True)

    def run(self):
        _out = self.output().open('w')
        _out.write('Hello!')
        _out.close()

if __name__ == '__main__':
    from luigi.mock import MockFile
    luigi.run(["--local-scheduler"], main_task_cls=SimpleTask)

