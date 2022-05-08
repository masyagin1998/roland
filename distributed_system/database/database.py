import asyncio
import logging
from random import randint, choice
from typing import Mapping, Any, List

import aiohttp
import yaml
from aiohttp import web

from utils.utils import create_arguments_parser, parse_args_as_dict, get_logger


class DataBase:
    OPERATORS = tuple({
        "Мегафон",
        "МТС",
        "Теле-2",
        "Yota",
        "Билайн",
        "Старлайн",
    })

    def __make_app(self) -> web.Application:
        app = web.Application()
        app.add_routes([
            web.post(self.EXEC_PATH, self.__exec),
        ])
        return app

    def __init__(self, cfg: dict, logger: logging.Logger):
        self.__loop = asyncio.new_event_loop()

        self.__cfg = cfg.copy()
        self.__logger = logger
        self.__host = self.__cfg['server']['host']
        self.__port = self.__cfg['server']['port']

        self.__min_timeout_ms = self.__cfg['server']['min_timeout_ms']
        self.__max_timeout_ms = self.__cfg['server']['max_timeout_ms']

        runner = web.AppRunner(self.__make_app())
        self.__loop.run_until_complete(runner.setup())
        site = web.TCPSite(runner, self.__host, self.__port)
        self.__loop.run_until_complete(site.start())

        self.__replicas: List[Mapping[str, Any]] = self.__cfg['replicas']
        self.__replicas_clients = [aiohttp.ClientSession('http://{}:{}'.format(
            rep['host'], rep['port']), loop=self.__loop) for rep in self.__replicas]
        self.__cur_replica_ind = 0

        self.__storage = {}

    def run(self):
        self.__loop.run_forever()

    BASE_PATH = '/api/v1'

    EXEC_PATH = BASE_PATH + '/exec'

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

    ADD_MNP_STMT = """
INSERT INTO
    mnp(phone_number, ts, operator_name)
VALUES
    ($1, NOW(), $2);
"""

    def __prepare_stmt(self, session_key: int, stmt: str, params: List[Any]):
        self.__logger.debug("""preparing stmt "{}" with params {} """.format(str(stmt).replace('\n', '\\n'), params),
                            extra={'session_key': session_key})
        self.__logger.debug("stmt has {} params".format(len(params)),
                            extra={'session_key': session_key})
        for i in range(len(params)):
            if type(params[i]) is str:
                p = "'{}'".format(params[i].replace("'", "\\'"))
            else:
                p = params[i]
            self.__logger.debug("param ${} is being replaced by value {}...".format(i + 1, p),
                                extra={'session_key': session_key})
            stmt = stmt.replace("$" + str(i + 1), p)
            self.__logger.debug("param ${} replaced successfully".format(i + 1),
                                extra={'session_key': session_key})
        self.__logger.debug("""statement prepared successfully to "{}" """.format(str(stmt).replace('\n', '\\n')),
                            extra={'session_key': session_key})
        return stmt

    def __gen_mnp_history(self):
        return [choice(self.OPERATORS) for __ in range(randint(1, 10))]

    async def __exec(self, req: web.Request) -> web.Response:
        self.__logger.info("got exec request",
                           extra={'session_key': "???"})
        try:
            params: dict = await req.json()
            session_key = params['session_key']
            stmt: str = params['stmt']
            params: List[Any] = params['params']
        except (ValueError, KeyError) as e:
            self.__logger.warning("unable to parse request!",
                                  extra={'session_key': "???"})
            return web.json_response({'code': -1, 'description': 'unable to parse request!'}, status=400)

        self.__logger.debug("exec request params: {}".format(str(await req.json()).replace('\n', '\\n')),
                            extra={'session_key': session_key})

        await asyncio.sleep(randint(self.__min_timeout_ms, self.__max_timeout_ms) / 1000.0)

        __ = self.__prepare_stmt(session_key, stmt, params)

        await asyncio.sleep(randint(self.__min_timeout_ms, self.__max_timeout_ms) / 1000.0)

        if stmt.startswith('\nSELECT'):
            phone_number = params[0]
            if phone_number not in self.__storage:
                self.__storage[phone_number] = self.__gen_mnp_history()
            res = self.__storage[phone_number]
            if stmt == self.GET_OPERATOR_STMT:
                return web.json_response({'data': res[-1], 'code': 0})
            elif stmt == self.GET_LATEST_MNP_STMT:
                data = None
                if len(res) > 1:
                    data = res[-1]
                return web.json_response({'data': data, 'code': 0})
            elif stmt == self.GET_MNP_HISTORY_STMT:
                return web.json_response({'data': res, 'code': 0})
        else:
            if stmt == self.ADD_MNP_STMT:
                phone_number = params[0]
                if phone_number not in self.__storage:
                    self.__storage[phone_number] = self.__gen_mnp_history()
                res = self.__storage[phone_number]
                res.append(params[1])
                self.__storage[phone_number] = res
                return web.json_response({'code': 0})

        self.__logger.warning("unable to parse request!",
                              extra={'session_key': session_key})
        return web.json_response({'code': -1, 'description': 'unable to parse request!'}, status=400)


desc_str = """DataBase mock."""


def parse_args() -> Mapping[str, Any]:
    parser = create_arguments_parser('mnp', desc_str)
    return parse_args_as_dict(parser)


def main():
    args = parse_args()
    with open(args['config'], 'r') as f:
        cfg = yaml.safe_load(f)

    cfg['server']['host'] = args.get('host', cfg['server']['host'])
    cfg['server']['port'] = args.get('port', cfg['server']['port'])

    logger = get_logger(cfg, 'database')
    db = DataBase(cfg, logger)
    db.run()


if __name__ == "__main__":
    main()
