import asyncio
import logging
from typing import Mapping, Any, List

import aiohttp
import yaml
from aiohttp import web

from utils.utils import create_arguments_parser, parse_args_as_dict, get_logger


class MNP:
    def __make_app(self) -> web.Application:
        app = web.Application()
        app.add_routes([
            web.post(self.GET_OPERATOR_PATH, self.__get_operator),
            web.post(self.GET_LATEST_MNP_PATH, self.__get_latest_mnp),
            web.post(self.GET_MNP_HISTORY_PATH, self.__get_mnp_history),
            web.post(self.ADD_MNP_PATH, self.__add_mnp),
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

        self.__rw_database: Mapping[str, Any] = self.__cfg['rw_database']
        self.__rw_database_client = aiohttp.ClientSession('http://{}:{}'.format(
            self.__rw_database['host'], self.__rw_database['port']), loop=self.__loop)

        self.__ro_databases: List[Mapping[str, Any]] = self.__cfg['ro_databases']
        self.__ro_databases_clients = [aiohttp.ClientSession('http://{}:{}'.format(
            db['host'], db['port']), loop=self.__loop) for db in self.__ro_databases]
        self.__cur_ro_database_ind = 0

        self.__ro_databases.append(self.__rw_database)
        self.__ro_databases_clients.append(self.__rw_database_client)

    def run(self):
        self.__loop.run_forever()

    def get_ro_db(self) -> (aiohttp.ClientSession, Mapping[str, Any]):
        cur_ro_database_ind = self.__cur_ro_database_ind
        self.__cur_ro_database_ind = (self.__cur_ro_database_ind + 1) % len(self.__ro_databases_clients)
        client = self.__ro_databases_clients[cur_ro_database_ind]
        cfg = self.__ro_databases[cur_ro_database_ind]
        return client, cfg

    BASE_PATH = '/api/v1'

    EXEC_PATH = BASE_PATH + '/exec'

    async def __to_db(self, client: aiohttp.ClientSession, stmt: str, params: List[Any]) -> aiohttp.ClientResponse:
        return await client.post(self.EXEC_PATH, json={'stmt': stmt, 'params': params})

    MNP_PATH = BASE_PATH + '/mnp'

    GET_OPERATOR_PATH = MNP_PATH + '/get_operator'

    GET_OPERATOR_STMT = """
SELECT
    mnp.operator_name
FROM
    mnp_schema.mnp
WHERE
    (mnp.phone_number = $1)
ORDER BY
    mnp.ts DESC
LIMIT 1;
"""

    async def __get_operator(self, req: web.Request) -> web.Response:
        self.__logger.info("got get_operator request")
        try:
            params: dict = await req.json()
            phone_number: str = params['phone_number']
        except (ValueError, KeyError) as e:
            self.__logger.warning("unable to parse request!")
            return web.json_response({'code': -1, 'description': 'unable to parse request!'}, status=400)

        self.__logger.debug("get_operator request params: {}".format(params))

        client, cfg = self.get_ro_db()
        self.__logger.debug("sending request to database {}:{}...".format(cfg['host'], cfg['port']))
        db_resp = await self.__to_db(client, self.GET_OPERATOR_STMT, [phone_number])
        self.__logger.debug("got response from database")

        print(await db_resp.json())

        return web.json_response(await db_resp.json(), status=db_resp.status)

    GET_LATEST_MNP_PATH = MNP_PATH + '/get_latest_mnp'

    GET_LATEST_MNP_STMT = """
SELECT
    mnp.operator_name
FROM
    mnp_schema.mnp
WHERE
    ((mnp.phone_number = $1) AND ((SELECT COUNT(*) FROM mnp_schema.mnp WHERE (mnp.phone_number = $1) > 1)))
ORDER BY
    mnp.ts DESC
LIMIT 1;
"""

    async def __get_latest_mnp(self, req: web.Request) -> web.Response:
        self.__logger.info("got get_latest_mnp request")
        try:
            params: dict = await req.json()
            phone_number: str = params['phone_number']
        except (ValueError, KeyError) as e:
            self.__logger.warning("unable to parse request!")
            return web.json_response({'code': -1, 'description': 'unable to parse request!'}, status=400)

        self.__logger.debug("get_latest_mnp request params: {}".format(params))

        client, cfg = self.get_ro_db()
        self.__logger.debug("sending request to database {}:{}...".format(cfg['host'], cfg['port']))
        db_resp = await self.__to_db(client, self.GET_LATEST_MNP_STMT, [phone_number])
        self.__logger.debug("got response from database")

        return web.json_response(await db_resp.json(), status=db_resp.status)

    GET_MNP_HISTORY_PATH = MNP_PATH + '/get_mnp_history'

    GET_MNP_HISTORY_STMT = """
SELECT
    mnp.operator_name
FROM
    mnp_schema.mnp
WHERE
    (mnp.phone_number = $1)
ORDER BY
    mnp.ts DESC;
"""

    async def __get_mnp_history(self, req: web.Request) -> web.Response:
        self.__logger.info("got get_mnp_history request")
        try:
            params: dict = await req.json()
            phone_number: str = params['phone_number']
        except (ValueError, KeyError) as e:
            self.__logger.warning("unable to parse request!")
            return web.json_response({'code': -1, 'description': 'unable to parse request!'}, status=400)

        self.__logger.debug("get_mnp_history request params: {}".format(params))

        client, cfg = self.get_ro_db()
        self.__logger.debug("sending request to database {}:{}...".format(cfg['host'], cfg['port']))
        db_resp = await self.__to_db(client, self.GET_MNP_HISTORY_STMT, [phone_number])
        self.__logger.debug("got response from database")

        return web.json_response(await db_resp.json(), status=db_resp.status)

    ADD_MNP_PATH = MNP_PATH + '/add_mnp'

    ADD_MNP_STMT = """
INSERT INTO
    mnp(phone_number, ts, operator_name)
VALUES
    ($1, NOW(), $2);
"""

    async def __add_mnp(self, req: web.Request) -> web.Response:
        self.__logger.info("got add_mnp request")
        try:
            params: dict = await req.json()
            phone_number: str = params['phone_number']
            operator_name: str = params['operator_name']
        except (ValueError, KeyError) as e:
            self.__logger.warning("unable to parse request!")
            return web.json_response({'code': -1, 'description': 'unable to parse request!'}, status=400)

        self.__logger.debug("add_mnp request params: {}".format(params))

        client, cfg = self.__rw_database_client, self.__rw_database
        self.__logger.debug("sending request to database {}:{}...".format(cfg['host'], cfg['port']))
        db_resp = await self.__to_db(client, self.ADD_MNP_STMT, [phone_number, operator_name])
        self.__logger.debug("got response from database")

        return web.json_response(await db_resp.json(), status=db_resp.status)


desc_str = """MNP server."""


def parse_args() -> Mapping[str, Any]:
    parser = create_arguments_parser('mnp', desc_str)
    return parse_args_as_dict(parser)


def main():
    args = parse_args()
    with open(args['config'], 'r') as f:
        cfg = yaml.safe_load(f)

    cfg['server']['host'] = args.get('host', cfg['server']['host'])
    cfg['server']['port'] = args.get('port', cfg['server']['port'])

    logger = get_logger(cfg, 'log_analyzer')
    mnp = MNP(cfg, logger)
    mnp.run()


if __name__ == "__main__":
    main()
