import json
from jsonschema import validate, ValidationError
import os
from concurrent.futures.process import ProcessPoolExecutor
from multiprocessing import Manager
from string import Template


from dataxsl import LoggingManager
from dataxsl.error import MissingParameterError
from dataxsl.register import PluginRegistry
from dataxsl.reader import Reader
from dataxsl.writer import Writer

logger = LoggingManager.get_logger(__name__)

# 生成随机密钥（16 字节 = 128 位）
key:bytes = b'TplEasT220241218'

class DataProcessor:
    def __init__(self, reader: Reader, writer: Writer, **kwargs):

        self.reader = reader
        self.writer = writer
        self.parallel = kwargs.get('parallel', 1)
        self.queue_size = kwargs.get('queue_size', 10)

    def start(self):

        # 1. 数据处理前调用
        self.pre_deal()

        # 2. 并行数据处理
        self.process_data()


        # 3. 数据处理后调用
        self.post_deal()

        # 4. invoke_hook 回调函数
        self.invoke_hook()

    def pre_deal(self):
        logger.info(f'do reader pre_deal')
        self.reader.pre_deal()

        logger.info(f'do writer pre_deal')
        self.writer.pre_deal()

    def post_deal(self):
        logger.info(f'do reader post_deal')
        self.reader.post_deal()

        logger.info(f'do writer post_deal')
        self.writer.post_deal()

    def invoke_hook(self):
        pass

    def process_data(self):

        with ProcessPoolExecutor(max_workers=self.parallel + 1) as executor:
            with Manager() as manager:
                queue = manager.Queue(self.queue_size)
                results = []
                # 提交任务读取任务
                future1 = executor.submit(self.reader.read_parallel, queue)
                results.append(future1)

                # 提交写入任务
                for i in range(self.parallel):
                    future2 = executor.submit(self.writer.writer_parallel, queue)
                    results.append(future2)

                # 等待作业返回
                for index, res in enumerate(results):
                    i, e = res.result()
                    logger.info(f'作业{index}, 作业状态={i}')
                    if i != 0:
                        raise e

class DataXslContext:

    def __init__(self):
        self.construct_config = {}
        self.reader_config = {}
        self.writer_config = {}
        self.process_config = {
            "channel": 1,
            "queue_size": 10,
            "parallel": 1
        }
        self.additive_attribute ={}

        # 定义JSON schema
        self.schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "$id": "https://example.com/dataxsl-job-schema.json",
            "title": "DataXSL Job Configuration Schema",
            "type": "object",
            "required": ["job"],
            "properties": {
                "job": {
                    "type": "object",
                    "required": ["content"],
                    "properties": {
                        "setting": {
                            "type": "object",
                            "properties": {
                                "channel": {
                                    "type": "integer",
                                    "minimum": 1,
                                    "maximum": 10,
                                    "description": "Number of parallel channels for processing"
                                },
                                "queue_size": {
                                    "type": "integer",
                                    "minimum": 1,
                                    "maximum": 1000,
                                    "description": "Size of the data queue",
                                    "default": 10
                                }
                            },
                        },
                        "content": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "required": ["reader", "writer"],
                                "properties": {
                                    "reader": {
                                        "type": "object",
                                        "properties": {
                                            "name": {
                                                "type": "string",
                                                "enum": ["excel_reader", "mysql_reader", "oracle_reader"],
                                                "description": "Name of the reader plugin"
                                            },
                                            "parameter": {
                                                "type": "object",
                                                "description": "reader parameter."
                                            }
                                        },
                                    },
                                    "writer": {
                                        "type": "object",
                                        "required": ["name"],
                                        "properties": {
                                            "name": {
                                                "type": "string",
                                                "enum": ["mysql_writer", "oracle_writer", "excel_writer"],
                                                "description": "Name of the writer plugin"
                                            },
                                            "parameter": {
                                                "type": "object",
                                                "description": "writer parameter."
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

    def _validate(self, config_data):
        # 使用JSON Schema验证
        try:
            validate(instance=config_data, schema=self.schema)
            logger.info("✅ JSON配置验证通过")
        except ValidationError as e:
            logger.error(f"❌ JSON配置验证失败: {e.message}")

    def parse(self, arguments):

        # 读取JSON文件内容为字符串
        if os.path.exists(arguments.job):
            with open(arguments.job, 'r', encoding="utf-8") as file:
                json_str = file.read()
        else:
            logger.warning(f"配置文件不存在: {arguments.job}.")
            exit(-1)

        # 读取参数
        if arguments.param is not None and len(arguments.param) > 0:
            config = dict(map(lambda x: x.split('='), arguments.param))
            logger.debug(f'params = {config}')
            # 使用Template进行变量替换（需要确保你的JSON字符串是可替换的）
            t = Template(json_str)
            # 注意：这里用的是safe_substitute以防键不存在导致错误
            json_str = t.safe_substitute(config)


        sf_config = json.loads(json_str)

        print_str = json.dumps(sf_config, indent=2, ensure_ascii=False)
        # logger.debug(f'job = {print_str}')
        # 验证json配置是否满足预期
        self._validate(sf_config)

        # 提取，reader，writer
        self.construct_config.setdefault('reader', sf_config['job']['content'][0]['reader']['name'])
        self.construct_config.setdefault('writer', sf_config['job']['content'][0]['writer']['name'])
        # 提取，reader和writer的参数
        self.reader_config = sf_config['job']['content'][0]['reader']['parameter']
        self.writer_config = sf_config['job']['content'][0]['writer']['parameter']
        # column 暂时用不到，先行剔除
        if self.reader_config['column']:
            self.reader_config.pop('column')

        # 如果设定了 channel 和 parallel 则需要做相关配置
        if sf_config['job']['setting']['channel'] is not None:
            self.process_config['channel'] = sf_config['job']['setting']['channel']
        if sf_config['job']['setting']['parallel'] is not None:
            self.reader_config['parallel'] = sf_config['job']['setting']['parallel']

        # 根据配置的数据进行构建，分别生成需要的reader和writer
        self.__construct__()

        return 0, None

    def __construct__(self):

        # construct_config = {
        #     "reader": "excel_reader",
        #     "writer.py": "mysql_writer"
        # }

        construct_config = self.construct_config
        reader_config = self.reader_config
        writer_config = self.writer_config
        process_config = self.process_config

        # 选择具体的 Reader 实现 (使用注册器)
        reader_name = construct_config.get('reader')
        if not reader_name:
            raise MissingParameterError('未设置reader参数，请检查配置文件')

        #print(PluginRegistry.list_readers())
        # 选择具体的 Reader 实现
        reader_cls = PluginRegistry.get_reader(reader_name)
        reader = reader_cls(**reader_config)
        # reader.validate(**reader_config)

        # 选择具体的 Writer 实现 (使用注册器)
        writer_name = construct_config.get('writer')
        if not writer_name:
            raise MissingParameterError('未设置writer参数，请检查配置文件')

        # 从注册器获取Writer类
        writer_cls = PluginRegistry.get_writer(writer_name)
        writer = writer_cls(**writer_config)
        # writer.validate(**writer_config)

        # 定义处理类，一个生产者对应多个消费者
        processor = DataProcessor(reader, writer, **process_config)
        # 启动process
        processor.start()

        return 0