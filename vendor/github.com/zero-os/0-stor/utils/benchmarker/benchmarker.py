from subprocess import Popen, PIPE
import json
from operator import add


class Nuker(object):
    def __init__(self, config):
        self.config = config
        self.data = {}

    def process_output(self, db, output):
        operations = self.data.get(db, {})

        for line in output.split('\n'):
            if not line:
                break
            op, num = line.split(',')
            op = op.replace('"', '')
            num = float(num.replace('"', ''))
            op_data = operations.get(op, [])
            op_data.append(num)
            operations[op] = op_data
        self.data[db] = operations

    def reduce_data(self):
        final = {}
        for db, value in self.data.iteritems():
            final[db] = {}
            for op, numbers in value.iteritems():
                length = len(numbers)
                result = reduce(add, numbers) / float(length)
                final[db][op] = result
        return final

    def graph(self):

        import numpy as np
        import matplotlib

        matplotlib.use('Agg')
        import matplotlib.pyplot as plt

        operations = tuple(self.config['operations'])
        databases = self.config["databases"].keys()
        colors = ['grey', 'black', 'yellow', 'green', 'blue', 'red']
        N = len(operations)
        ind = np.arange(N)  # the x locations for the groups
        width = 0.27  # the width of the bars

        fig = plt.figure()
        ax = fig.add_subplot(111)

        recatangles = []
        for db in databases:
            r = []
            for op in operations:
                r.append(self.data[db][op])
            recatangles.append(r)

        rec_objs = []
        for i, r in enumerate(recatangles):
            rec = ax.bar(ind + width * i, r, width, color=colors.pop())
            rec_objs.append(rec)

        ax.set_ylabel('Requests/s')
        ax.set_xticks(ind + width)
        ax.set_xticklabels(tuple(operations))
        ax.legend(tuple([r[0] for r in rec_objs]), tuple(databases))

        def autolabel(rects):
            for rect in rects:
                h = rect.get_height()
                ax.text(rect.get_x() + rect.get_width() / 2., 1.05 * h, '%d' % int(h),
                        ha='center', va='bottom')
        for r in rec_objs:
            autolabel(r)

        plt.savefig('result.png')
        plt.show()

    def nuke(self):
        databases = self.config["databases"].keys()
        for db in databases:
            print '\n\n*********************************'
            print '[ %s ] START Benchmarking' % db
            print '*********************************\n\n'
            for i in range(self.config['number_of_iterations_per_db']):
                print '\t\t------(Iteraton: %s)--------\n' % str(i+1)
                cmd = ["redis-benchmark",
                                 "-h",
                                 self.config["databases"][db]['host'],
                                 "-p",
                                 str(self.config["databases"][db]['port']),
                                 "-r",
                                 str(self.config['keyspace_length']),
                                 "-n",
                                 str(self.config['number_of_requests']),
                                 "-t",
                                 ','.join(self.config['operations']),
                                 "--csv"]
                if self.config['quiet']:
                    cmd.append('-q')

                if self.config['number_of_pipelined_requests']:
                    cmd.append('-P')
                    cmd.append(str(self.config['number_of_pipelined_requests']))

                process = Popen(cmd, stdout=PIPE)
                (output, err) = process.communicate()
                self.process_output(db, output)
                exit_code = process.wait()
                print output


def run():
    config = json.load(open('config.json'))
    n = Nuker(config)
    n.nuke()

    print '*******************'
    print 'SUMMARY'
    print '*******************\n\n'
    print '---Data across all iterations----\n\n'
    print n.data
    n.data = n.reduce_data()
    print '\n\n---Average----\n\n'
    print n.data
    print '\n\n'
    n.graph()

if __name__ == '__main__':
    run()