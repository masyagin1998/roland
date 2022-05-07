#!/usr/bin/python3
import asyncio
import logging
from typing import Mapping, Any
from urllib import parse

import yaml
from aiohttp import web

from utils.utils import get_logger, create_arguments_parser, parse_args_as_dict


class LogAnalyzer:
    def __make_app(self) -> web.Application:
        app = web.Application()
        app.add_routes([
            web.post(self.ADD_LOG_PATH, self.__add_log),
        ])
        return app

    def __init__(self, cfg: dict, logger: logging.Logger):
        self.__loop = asyncio.new_event_loop()

        self.__cfg = cfg.copy()
        self.__logger = logger
        self.__host = self.__cfg['server']['host']
        self.__port = self.__cfg['server']['port']

        runner = web.AppRunner(self.__make_app())
        self.__loop.run_until_complete(runner.setup())
        site = web.TCPSite(runner, self.__host, self.__port)
        self.__loop.run_until_complete(site.start())

    def run(self):
        self.__loop.run_forever()

    BASE_PATH = '/api/v1'

    ADD_LOG_PATH = BASE_PATH + '/add_log'

    async def __add_log(self, req: web.Request) -> web.Response:
        self.__logger.info('got new log request')
        try:
            params = parse.parse_qs(await req.text())
        except (ValueError, KeyError) as e:
            self.__logger.warning("unable to parse request!")
            return web.json_response({'code': -1, 'description': 'unable to parse request!'}, status=400)

        # self.__logger.debug('new log request params: {}'.format(params))
        params = self.__clean_params(params)
        self.__logger.debug('new log request params: {}'.format(params))

        self.__logger.info('successfully processed new log')

        return web.json_response({'code': 0})

    def __clean_params(self, params: dict):
        return {k: v[0] for k, v in params.items() if k in ['name', 'msg', 'levelname', 'asctime']}


desc_str = """Log analysis server."""


def parse_args() -> Mapping[str, Any]:
    parser = create_arguments_parser('log_analyzer', desc_str)
    return parse_args_as_dict(parser)


def main():
    args = parse_args()
    with open(args['config'], 'r') as f:
        cfg = yaml.safe_load(f)

    cfg['server']['host'] = args.get('host', cfg['server']['host'])
    cfg['server']['port'] = args.get('port', cfg['server']['port'])

    logger = get_logger(cfg, 'log_analyzer')
    log_analyzer = LogAnalyzer(cfg, logger)
    log_analyzer.run()


if __name__ == "__main__":
    main()
