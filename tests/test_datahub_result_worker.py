import os
import time
import unittest2 as unittest
import logging.config
logging.config.fileConfig("pyspider/logging.conf")

import shutil
from pyspider.database.sqlite import resultdb
from pyspider.result import DataHubResultWorker
from pyspider.libs.multiprocessing_queue import Queue
from pyspider.libs.utils import run_in_thread

class TestDataHubResultWorker(unittest.TestCase):
    resultdb_path = './data/tests/result.db'

    @classmethod
    def setUpClass(self):
        shutil.rmtree('./data/tests/', ignore_errors=True)
        os.makedirs('./data/tests/')

        def get_resultdb():
            return resultdb.ResultDB(self.resultdb_path)

        self.resultdb = get_resultdb()
        self.inqueue = Queue(10)

        def run_result_worker():
            self.result_worker = DataHubResultWorker(get_resultdb(), self.inqueue)
            self.result_worker.run()

        self.process = run_in_thread(run_result_worker)
        time.sleep(1)

    @classmethod
    def tearDownClass(self):
        if self.process.is_alive():
            self.result_worker.quit()
            self.process.join(2)
        assert not self.process.is_alive()
        shutil.rmtree('./data/tests/', ignore_errors=True)

    def test_20_insert_result(self):
        data = {
            'a': 'b'
        }
        self.inqueue.put(({
            'project': 'test_project',
            'taskid': 'id1',
            'url': 'url1'
        }, data))
        time.sleep(1)
        self.resultdb._list_project()
        self.assertEqual(len(self.resultdb.projects), 1)
        self.assertEqual(self.resultdb.count('test_project'), 1)

        result = self.resultdb.get('test_project', 'id1')
        self.assertEqual(result['result'], data)

    def test_40_insert_list(self):
        self.inqueue.put(({
            'project': 'test_project',
            'taskid': 'id2',
            'url': 'url1'
        }, [{'a':'a'}, {'a':'b'}, {'a':'c'}, {'a': 'd'}, {'a':'e'}]))
        time.sleep(1)
        result = self.resultdb.get('test_project', 'id2')