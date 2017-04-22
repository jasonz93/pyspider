from .result_worker import ResultWorker
from datahub import DataHub
from datahub.models import TupleRecord, Topic
import os

class DataHubResultWorker(ResultWorker):
    def __init__(self, resultdb, inqueue):
        super(DataHubResultWorker, self).__init__(resultdb, inqueue)
        accessKey = os.environ['DATAHUB_ACCESS_KEY']
        accessSecret = os.environ['DATAHUB_ACCESS_SECRET']
        endpoint = os.environ['DATAHUB_ENDPOINT']
        self.project = os.environ['DATAHUB_PROJECT']
        self.datahub = DataHub(accessKey, accessSecret, endpoint)
        self.topicInfos = {}

    def on_result(self, task, result):
        """
        
        :param task: 
        :type result: dict 
        :return: 
        """
        super(DataHubResultWorker, self).on_result(task, result)
        topicName = task['project']
        if not self.topicInfos.has_key(topicName):
            self.datahub.wait_shards_ready(self.project, topicName)
            topicInfo = {
                'topic': self.datahub.get_topic(topicName, self.project),
                'shards': self.datahub.list_shards(self.project, topicName),
                'current': 0
            }
            self.topicInfos[topicName] = topicInfo
        else:
            topicInfo = self.topicInfos[topicName]
        topic = topicInfo['topic']
        """
        :type: Topic
        """

        records = []

        values = []
        for field in topic.record_schema.fields:
            if (result.has_key(field.name)):
                values.append(result[field.name])
            else:
                values.append(None)
        record = TupleRecord(schema=topic.record_schema, values=values)
        record.shard_id = topicInfo['shards'][topicInfo['current']].shard_id
        topicInfo['current'] += 1
        if topicInfo['current'] >= len(topicInfo['shards']):
            topicInfo['current'] = 0
        records.append(record)

        failed_indexes = self.datahub.put_records(self.project, topicName, records)
        print "put tuple %d records, failed list: %s" %(len(records), failed_indexes)