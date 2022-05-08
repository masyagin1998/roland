#!/usr/bin/python3
import asyncio
import base64
import logging
from random import randint
from typing import Mapping, Any, List

import aiohttp
import aiohttp_session as web_session
import yaml
from aiohttp import web
from aiohttp_session.cookie_storage import EncryptedCookieStorage
from cryptography import fernet

from utils.utils import get_logger, create_arguments_parser, parse_args_as_dict


class Balancer:
    def __make_app(self) -> web.Application:
        app = web.Application()
        fernet_key = fernet.Fernet.generate_key()
        secret_key = base64.urlsafe_b64decode(fernet_key)
        web_session.setup(app, EncryptedCookieStorage(secret_key))
        app.add_routes([
            web.post(self.LOGIN_PATH, self.__login),
            web.post(self.LOGOUT_PATH, self.__logout),
            web.route('*', self.TRANSFER_PATH + '/{tail:.*}', self.__transfer),
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

        self.__mnps: List[Mapping[str, Any]] = self.__cfg['mnps']

        self.__mnp_clients = [aiohttp.ClientSession('http://{}:{}'.format(mnp['host'], mnp['port']), loop=self.__loop)
                              for mnp in self.__mnps]
        self.__cur_mnp_ind = 0

        self.__cur_session_key = 1

    def run(self):
        self.__loop.run_forever()

    BASE_PATH = '/api/v1'

    LOGIN_PATH = BASE_PATH + '/login'

    async def __login(self, req: web.Request) -> web.Response:
        self.__logger.info("got login request",
                           extra={'session_key': "???"})
        session = await web_session.get_session(req)
        if 'logged_in' in session:
            self.__logger.warning("user '{}' is already logged in!".format(session['login']),
                                  extra={'session_key': session['session_key']})
            return web.json_response({'code': -1, 'description': 'already logged in!'}, status=401)
        try:
            params: dict = await req.json()
            login: str = params['login']
            password: str = params['password']
        except (ValueError, KeyError) as e:
            self.__logger.warning("unable to parse request!",
                                  extra={'session_key': "???"})
            return web.json_response({'code': -1, 'description': 'unable to parse request!'}, status=400)

        self.__logger.debug("login request params: {}".format(params),
                            extra={'session_key': "???"})

        await asyncio.sleep(randint(self.__min_timeout_ms, self.__max_timeout_ms) / 1000.0)

        if login != password:
            self.__logger.warning("invalid login or password!",
                                  extra={'session_key': "???"})
            return web.json_response({'code': -1, 'description': 'invalid login or password!'}, status=401)

        session['login'] = login
        session['password'] = password
        session['logged_in'] = True
        session['session_key'] = self.__cur_session_key
        self.__cur_session_key += 1

        self.__logger.info("user '{}' successfully logged in".format(login),
                           extra={'session_key': session['session_key']})

        return web.json_response({'code': 0})

    TRANSFER_PATH = BASE_PATH

    async def __transfer_inner(self, client: aiohttp.ClientSession, s_session: web_session.Session,
                               s_req: web.Request) -> aiohttp.ClientResponse:
        headers = s_req.headers.copy()
        del headers['Content-Length']
        data = (await s_req.json()).copy()
        data['session_key'] = s_session['session_key']
        return await client.request(s_req.method, s_req.path, headers=headers, json=data)

    async def __transfer_mnp(self, session: web_session.Session, req: web.Request) -> web.Response:
        cur_mnp_ind = self.__cur_mnp_ind
        self.__cur_mnp_ind = (self.__cur_mnp_ind + 1) % len(self.__mnp_clients)
        client = self.__mnp_clients[cur_mnp_ind]

        self.__logger.debug(
            "transferring {} request with path '{}' to MNP server {}:{}...".format(
                req.method, req.path, self.__mnps[cur_mnp_ind]['host'], self.__mnps[cur_mnp_ind]['port']),
            extra={'session_key': session['session_key']})
        c_resp = await self.__transfer_inner(client, session, req)
        self.__logger.debug("transferred request successfully",
                            extra={'session_key': session['session_key']})

        print(await c_resp.json())

        return web.json_response(await c_resp.json(), status=c_resp.status)

    async def __transfer(self, req: web.Request) -> web.Response:
        path = req.path.replace('/api/v1/', '', 1)
        self.__logger.debug("{} request with path '{}' needs transfer...".format(req.method, path),
                            extra={'session_key': "???"})
        session = await web_session.get_session(req)
        if 'logged_in' not in session:
            self.__logger.warning('user is not logged in!',
                                  extra={'session_key': "???"})
            return web.json_response({'code': -1, 'description': 'user is not logged in!'}, status=401)

        path_parts = path.split('/')
        base = path_parts[0]
        if base == 'mnp':
            return await self.__transfer_mnp(session, req)
        else:
            self.__logger.warning('could not transfer request!',
                                  extra={'session_key': session['session_key']})
            return web.json_response({'code': -1, 'description': 'could not transfer request!'}, status=400)

    LOGOUT_PATH = BASE_PATH + '/logout'

    async def __logout(self, req: web.Request) -> web.Response:
        self.__logger.info("got logout request")
        session = await web_session.get_session(req)
        if 'logged_in' not in session:
            self.__logger.warning('user is not logged in!',
                                  extra={'session_key': "???"})
            return web.json_response({'code': -1, 'description': 'user is not logged in!'}, status=401)

        login = session['login']
        session.clear()

        self.__logger.info("user '{}' successfully logged out".format(login),
                           extra={'session_key': session['session_key']})

        return web.json_response({'code': 0})


desc_str = """Authorization and load-balancing server."""


def parse_args() -> Mapping[str, Any]:
    parser = create_arguments_parser('balancer', desc_str)
    return parse_args_as_dict(parser)


def main():
    args = parse_args()
    with open(args['config'], 'r') as f:
        cfg = yaml.safe_load(f)

    cfg['server']['host'] = args.get('host', cfg['server']['host'])
    cfg['server']['port'] = args.get('port', cfg['server']['port'])

    logger = get_logger(cfg, 'balancer')
    balancer = Balancer(cfg, logger)
    balancer.run()


if __name__ == "__main__":
    main()
