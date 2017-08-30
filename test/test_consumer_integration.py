import logging
import os
import time

from mock import patch
import pytest
from kafka.vendor import six
from kafka.vendor.six.moves import range

from kafka import KafkaConsumer, KafkaProducer
import kafka.codec
from kafka.errors import (
     KafkaTimeoutError, UnsupportedCodecError, UnsupportedVersionError
)
from kafka.structs import TopicPartition, OffsetAndTimestamp

from test.fixtures import ZookeeperFixture, KafkaFixture, random_string, version
from test.testutil import KafkaIntegrationTestCase, kafka_versions, Timer


@pytest.mark.skipif(not version(), reason="No KAFKA_VERSION set")
def test_kafka_consumer(kafka_producer, topic, kafka_consumer_factory):
    """Test KafkaConsumer"""
    kafka_consumer = kafka_consumer_factory(auto_offset_reset='earliest')

    # TODO replace this with a `send_messages()` pytest fixture
    # as we will likely need this elsewhere
    for i in range(0, 100):
        kafka_producer.send(topic, partition=0, value=str(i).encode())
    for i in range(100, 200):
        kafka_producer.send(topic, partition=1, value=str(i).encode())
    kafka_producer.flush()

    cnt = 0
    messages = {0: set(), 1: set()}
    for message in kafka_consumer:
        logging.debug("Consumed message %s", repr(message))
        cnt += 1
        messages[message.partition].add(message.offset)
        if cnt >= 200:
            break

    assert len(messages[0]) == 100
    assert len(messages[1]) == 100
    kafka_consumer.close()


@pytest.mark.skipif(not version(), reason="No KAFKA_VERSION set")
def test_kafka_consumer_unsupported_encoding(
        topic, kafka_producer_factory, kafka_consumer_factory):
    # Send a compressed message
    producer = kafka_producer_factory(compression_type="gzip")
    fut = producer.send(topic, b"simple message" * 200)
    fut.get(timeout=5)
    producer.close()

    # Consume, but with the related compression codec not available
    with patch.object(kafka.codec, "has_gzip") as mocked:
        mocked.return_value = False
        consumer = kafka_consumer_factory(auto_offset_reset='earliest')
        error_msg = "Libraries for gzip compression codec not found"
        with pytest.raises(UnsupportedCodecError, match=error_msg):
            consumer.poll(timeout_ms=2000)


class TestConsumerIntegration(KafkaIntegrationTestCase):
    maxDiff = None

    @classmethod
    def setUpClass(cls):
        if not os.environ.get('KAFKA_VERSION'):
            return

        cls.zk = ZookeeperFixture.instance()
        chroot = random_string(10)
        cls.server1 = KafkaFixture.instance(0, cls.zk,
                                            zk_chroot=chroot)
        cls.server2 = KafkaFixture.instance(1, cls.zk,
                                            zk_chroot=chroot)

        cls.server = cls.server1 # Bootstrapping server

    @classmethod
    def tearDownClass(cls):
        if not os.environ.get('KAFKA_VERSION'):
            return

        cls.server1.close()
        cls.server2.close()
        cls.zk.close()

    def send_messages(self, partition, messages):
        messages = [ create_message(self.msg(str(msg))) for msg in messages ]
        produce = ProduceRequestPayload(self.topic, partition, messages = messages)
        resp, = self.client.send_produce_request([produce])
        self.assertEqual(resp.error, 0)

        return [ x.value for x in messages ]

    def send_gzip_message(self, partition, messages):
        message = create_gzip_message([(self.msg(str(msg)), None) for msg in messages])
        produce = ProduceRequestPayload(self.topic, partition, messages = [message])
        resp, = self.client.send_produce_request([produce])
        self.assertEqual(resp.error, 0)

    def assert_message_count(self, messages, num_messages):
        # Make sure we got them all
        self.assertEqual(len(messages), num_messages)

        # Make sure there are no duplicates
        self.assertEqual(len(set(messages)), num_messages)

    def kafka_consumer(self, **configs):
        brokers = '%s:%d' % (self.server.host, self.server.port)
        consumer = KafkaConsumer(self.topic,
                                 bootstrap_servers=brokers,
                                 **configs)
        return consumer

    def kafka_producer(self, **configs):
        brokers = '%s:%d' % (self.server.host, self.server.port)
        producer = KafkaProducer(
            bootstrap_servers=brokers, **configs)
        return producer

    def test_kafka_consumer__blocking(self):
        TIMEOUT_MS = 500
        consumer = self.kafka_consumer(auto_offset_reset='earliest',
                                       enable_auto_commit=False,
                                       consumer_timeout_ms=TIMEOUT_MS)

        # Manual assignment avoids overhead of consumer group mgmt
        consumer.unsubscribe()
        consumer.assign([TopicPartition(self.topic, 0)])

        # Ask for 5 messages, nothing in queue, block 500ms
        with Timer() as t:
            with self.assertRaises(StopIteration):
                msg = next(consumer)
        self.assertGreaterEqual(t.interval, TIMEOUT_MS / 1000.0 )

        self.send_messages(0, range(0, 10))

        # Ask for 5 messages, 10 in queue. Get 5 back, no blocking
        messages = set()
        with Timer() as t:
            for i in range(5):
                msg = next(consumer)
                messages.add((msg.partition, msg.offset))
        self.assertEqual(len(messages), 5)
        self.assertLess(t.interval, TIMEOUT_MS / 1000.0 )

        # Ask for 10 messages, get 5 back, block 500ms
        messages = set()
        with Timer() as t:
            with self.assertRaises(StopIteration):
                for i in range(10):
                    msg = next(consumer)
                    messages.add((msg.partition, msg.offset))
        self.assertEqual(len(messages), 5)
        self.assertGreaterEqual(t.interval, TIMEOUT_MS / 1000.0 )
        consumer.close()

    @kafka_versions('>=0.8.1')
    def test_kafka_consumer__offset_commit_resume(self):
        GROUP_ID = random_string(10)

        self.send_messages(0, range(0, 100))
        self.send_messages(1, range(100, 200))

        # Start a consumer
        consumer1 = self.kafka_consumer(
            group_id=GROUP_ID,
            enable_auto_commit=True,
            auto_commit_interval_ms=100,
            auto_offset_reset='earliest',
        )

        # Grab the first 180 messages
        output_msgs1 = []
        for _ in range(180):
            m = next(consumer1)
            output_msgs1.append(m)
        self.assert_message_count(output_msgs1, 180)
        consumer1.close()

        # The total offset across both partitions should be at 180
        consumer2 = self.kafka_consumer(
            group_id=GROUP_ID,
            enable_auto_commit=True,
            auto_commit_interval_ms=100,
            auto_offset_reset='earliest',
        )

        # 181-200
        output_msgs2 = []
        for _ in range(20):
            m = next(consumer2)
            output_msgs2.append(m)
        self.assert_message_count(output_msgs2, 20)
        self.assertEqual(len(set(output_msgs1) | set(output_msgs2)), 200)
        consumer2.close()

    @kafka_versions('>=0.10.1')
    def test_kafka_consumer_max_bytes_simple(self):
        self.send_messages(0, range(100, 200))
        self.send_messages(1, range(200, 300))

        # Start a consumer
        consumer = self.kafka_consumer(
            auto_offset_reset='earliest', fetch_max_bytes=300)
        seen_partitions = set([])
        for i in range(10):
            poll_res = consumer.poll(timeout_ms=100)
            for partition, msgs in six.iteritems(poll_res):
                for msg in msgs:
                    seen_partitions.add(partition)

        # Check that we fetched at least 1 message from both partitions
        self.assertEqual(
            seen_partitions, set([
                TopicPartition(self.topic, 0), TopicPartition(self.topic, 1)]))
        consumer.close()

    @kafka_versions('>=0.10.1')
    def test_kafka_consumer_max_bytes_one_msg(self):
        # We send to only 1 partition so we don't have parallel requests to 2
        # nodes for data.
        self.send_messages(0, range(100, 200))

        # Start a consumer. FetchResponse_v3 should always include at least 1
        # full msg, so by setting fetch_max_bytes=1 we should get 1 msg at a time
        # But 0.11.0.0 returns 1 MessageSet at a time when the messages are
        # stored in the new v2 format by the broker.
        #
        # DP Note: This is a strange test. The consumer shouldn't care
        # how many messages are included in a FetchResponse, as long as it is
        # non-zero. I would not mind if we deleted this test. It caused
        # a minor headache when testing 0.11.0.0.
        group = 'test-kafka-consumer-max-bytes-one-msg-' + random_string(5)
        consumer = self.kafka_consumer(
            group_id=group,
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000,
            fetch_max_bytes=1)

        fetched_msgs = [next(consumer) for i in range(10)]
        self.assertEqual(len(fetched_msgs), 10)
        consumer.close()

    @kafka_versions('>=0.10.1')
    def test_kafka_consumer_offsets_for_time(self):
        late_time = int(time.time()) * 1000
        middle_time = late_time - 1000
        early_time = late_time - 2000
        tp = TopicPartition(self.topic, 0)

        timeout = 10
        kafka_producer = self.kafka_producer()
        early_msg = kafka_producer.send(
            self.topic, partition=0, value=b"first",
            timestamp_ms=early_time).get(timeout)
        late_msg = kafka_producer.send(
            self.topic, partition=0, value=b"last",
            timestamp_ms=late_time).get(timeout)

        consumer = self.kafka_consumer()
        offsets = consumer.offsets_for_times({tp: early_time})
        self.assertEqual(len(offsets), 1)
        self.assertEqual(offsets[tp].offset, early_msg.offset)
        self.assertEqual(offsets[tp].timestamp, early_time)

        offsets = consumer.offsets_for_times({tp: middle_time})
        self.assertEqual(offsets[tp].offset, late_msg.offset)
        self.assertEqual(offsets[tp].timestamp, late_time)

        offsets = consumer.offsets_for_times({tp: late_time})
        self.assertEqual(offsets[tp].offset, late_msg.offset)
        self.assertEqual(offsets[tp].timestamp, late_time)

        offsets = consumer.offsets_for_times({})
        self.assertEqual(offsets, {})

        # Out of bound timestamps check

        offsets = consumer.offsets_for_times({tp: 0})
        self.assertEqual(offsets[tp].offset, early_msg.offset)
        self.assertEqual(offsets[tp].timestamp, early_time)

        offsets = consumer.offsets_for_times({tp: 9999999999999})
        self.assertEqual(offsets[tp], None)

        # Beginning/End offsets

        offsets = consumer.beginning_offsets([tp])
        self.assertEqual(offsets, {
            tp: early_msg.offset,
        })
        offsets = consumer.end_offsets([tp])
        self.assertEqual(offsets, {
            tp: late_msg.offset + 1
        })
        consumer.close()

    @kafka_versions('>=0.10.1')
    def test_kafka_consumer_offsets_search_many_partitions(self):
        tp0 = TopicPartition(self.topic, 0)
        tp1 = TopicPartition(self.topic, 1)

        kafka_producer = self.kafka_producer()
        send_time = int(time.time() * 1000)
        timeout = 10
        p0msg = kafka_producer.send(
            self.topic, partition=0, value=b"XXX",
            timestamp_ms=send_time).get(timeout)
        p1msg = kafka_producer.send(
            self.topic, partition=1, value=b"XXX",
            timestamp_ms=send_time).get(timeout)

        consumer = self.kafka_consumer()
        offsets = consumer.offsets_for_times({
            tp0: send_time,
            tp1: send_time
        })

        self.assertEqual(offsets, {
            tp0: OffsetAndTimestamp(p0msg.offset, send_time),
            tp1: OffsetAndTimestamp(p1msg.offset, send_time)
        })

        offsets = consumer.beginning_offsets([tp0, tp1])
        self.assertEqual(offsets, {
            tp0: p0msg.offset,
            tp1: p1msg.offset
        })

        offsets = consumer.end_offsets([tp0, tp1])
        self.assertEqual(offsets, {
            tp0: p0msg.offset + 1,
            tp1: p1msg.offset + 1
        })
        consumer.close()

    @kafka_versions('<0.10.1')
    def test_kafka_consumer_offsets_for_time_old(self):
        consumer = self.kafka_consumer()
        tp = TopicPartition(self.topic, 0)

        with self.assertRaises(UnsupportedVersionError):
            consumer.offsets_for_times({tp: int(time.time())})

    @kafka_versions('>=0.10.1')
    def test_kafka_consumer_offsets_for_times_errors(self):
        consumer = self.kafka_consumer(fetch_max_wait_ms=200,
                                       request_timeout_ms=500)
        tp = TopicPartition(self.topic, 0)
        bad_tp = TopicPartition(self.topic, 100)

        with self.assertRaises(ValueError):
            consumer.offsets_for_times({tp: -1})

        with self.assertRaises(KafkaTimeoutError):
            consumer.offsets_for_times({bad_tp: 0})
