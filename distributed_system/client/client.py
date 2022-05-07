#!/usr/bin/python3
import sys
from enum import Enum
from typing import Mapping, Any

import requests
import yaml

from utils.utils import parse_args_as_dict, create_arguments_parser


class ManualClient:
    class Command(Enum):
        LOGIN = 1
        GET_OPERATOR = 2
        GET_LATEST_MNP = 3
        GET_MNP_HISTORY = 4
        ADD_MNP = 5
        LOGOUT = 6

    class State(Enum):
        READ_COMMAND = 1
        READ_LOGIN = 2
        READ_PASSWORD = 3
        READ_PHONE_NUMBER = 4
        READ_OPERATOR_NAME = 5

    class CurState:
        def __init__(self):
            self.active = True
            self.state = ManualClient.State.READ_COMMAND
            self.command = None

            self.login = None
            self.password = None

            self.phone_number = None
            self.operator_name = None

            self.ready = False

    def __init__(self, cfg: dict):
        self.__cfg = cfg.copy()
        self.__server_host = self.__cfg['server']['host']
        self.__server_port = self.__cfg['server']['port']

        self.__url_base = 'http://{}:{}'.format(self.__server_host, self.__server_port)

        self.__cur_state = self.CurState()

        self.__session = requests.Session()

    def __print_cur_state(self):
        if self.__cur_state.state == self.State.READ_COMMAND:
            print("""Read command:
1 - login;
2 - get operator for phone number;
3 - get latest mnp for phone number;
4 - get mnp history for phone number;
5 - add mnp for phone number;
6 - logout;
7 - exit""")
        elif self.__cur_state.state == self.State.READ_LOGIN:
            print("""Read login:""")
        elif self.__cur_state.state == self.State.READ_PASSWORD:
            print("""Read password:""")
        elif self.__cur_state.state == self.State.READ_PHONE_NUMBER:
            print("""Read phone number:""")
        elif self.__cur_state.state == self.State.READ_OPERATOR_NAME:
            print("""Read operator name:""")
        else:
            print("Impossible state!")
            sys.exit(1)

    def __switch_cur_state(self, s: str) -> None:
        # READ_COMMAND
        if self.__cur_state.state == self.State.READ_COMMAND:
            v = int(s)
            if v == 1:
                self.__cur_state.command = self.Command.LOGIN
                self.__cur_state.state = self.State.READ_LOGIN
            elif v == 2:
                self.__cur_state.command = self.Command.GET_OPERATOR
                self.__cur_state.state = self.State.READ_PHONE_NUMBER
            elif v == 3:
                self.__cur_state.command = self.Command.GET_LATEST_MNP
                self.__cur_state.state = self.State.READ_PHONE_NUMBER
            elif v == 4:
                self.__cur_state.command = self.Command.GET_MNP_HISTORY
                self.__cur_state.state = self.State.READ_PHONE_NUMBER
            elif v == 5:
                self.__cur_state.command = self.Command.ADD_MNP
                self.__cur_state.state = self.State.READ_PHONE_NUMBER
            elif v == 6:
                self.__cur_state.command = self.Command.LOGOUT
                self.__cur_state.ready = True
            elif v == 7:
                print("Client stopped")
                return
            else:
                print("Unknown command \"{}\"!".format(s))
                return
        # READ_LOGIN
        elif self.__cur_state.state == self.State.READ_LOGIN:
            self.__cur_state.login = s
            if self.__cur_state.command == self.Command.LOGIN:
                self.__cur_state.state = self.State.READ_PASSWORD
        # READ_PASSWORD
        elif self.__cur_state.state == self.State.READ_PASSWORD:
            self.__cur_state.password = s
            if self.__cur_state.command == self.Command.LOGIN:
                self.__cur_state.ready = True
        # READ_PHONE_NUMBER
        elif self.__cur_state.state == self.State.READ_PHONE_NUMBER:
            self.__cur_state.phone_number = s
            if self.__cur_state.command == self.Command.GET_OPERATOR:
                self.__cur_state.ready = True
            elif self.__cur_state.command == self.Command.GET_LATEST_MNP:
                self.__cur_state.ready = True
            elif self.__cur_state.command == self.Command.GET_MNP_HISTORY:
                self.__cur_state.ready = True
            elif self.__cur_state.command == self.Command.ADD_MNP:
                self.__cur_state.state = self.State.READ_OPERATOR_NAME
        # READ OPERATOR NAME
        elif self.__cur_state.state == self.State.READ_OPERATOR_NAME:
            self.__cur_state.operator_name = s
            if self.__cur_state.command == self.Command.ADD_MNP:
                self.__cur_state.ready = True

    BASE_PATH = '/api/v1'

    LOGIN_PATH = BASE_PATH + '/login'

    def __login(self):
        resp = self.__session.post(
            self.__url_base + self.LOGIN_PATH,
            json={
                'login': self.__cur_state.login,
                'password': self.__cur_state.password,
            }
        )
        print(resp)
        print(resp.text)

    MNP_PATH = BASE_PATH + '/mnp'

    GET_OPERATOR_PATH = MNP_PATH + '/get_operator'

    def __get_operator(self):
        resp = self.__session.post(
            self.__url_base + self.GET_OPERATOR_PATH,
            json={
                'phone_number': self.__cur_state.phone_number
            }
        )
        print(resp)
        print(resp.json())

    GET_LATEST_MNP_PATH = MNP_PATH + '/get_latest_mnp'

    def __get_latest_mnp(self):
        resp = self.__session.post(
            self.__url_base + self.GET_LATEST_MNP_PATH,
            json={
                'phone_number': self.__cur_state.phone_number
            }
        )
        print(resp)
        print(resp.json())

    GET_MNP_HISTORY_PATH = MNP_PATH + '/get_mnp_history'

    def __get_mnp_history(self):
        resp = self.__session.post(
            self.__url_base + self.GET_MNP_HISTORY_PATH,
            json={
                'phone_number': self.__cur_state.phone_number
            }
        )
        print(resp)
        print(resp.json())

    ADD_MNP_PATH = MNP_PATH + '/add_mnp'

    def __add_mnp(self):
        resp = self.__session.post(
            self.__url_base + self.ADD_MNP_PATH,
            json={
                'phone_number': self.__cur_state.phone_number,
                'operator_name': self.__cur_state.operator_name
            }
        )
        print(resp)
        print(resp.json())

    LOGOUT_PATH = MNP_PATH + '/logout'

    def __logout(self):
        resp = self.__session.post(
            self.__url_base + self.LOGOUT_PATH
        )
        print(resp)
        print(resp.json())

    def __send_request(self) -> None:
        if not self.__cur_state.ready:
            return

        if self.__cur_state.command == self.Command.LOGIN:
            self.__login()
        elif self.__cur_state.command == self.Command.GET_OPERATOR:
            self.__get_operator()
        elif self.__cur_state.command == self.Command.GET_LATEST_MNP:
            self.__get_latest_mnp()
        elif self.__cur_state.command == self.Command.GET_MNP_HISTORY:
            self.__get_mnp_history()
        elif self.__cur_state.command == self.Command.ADD_MNP:
            self.__add_mnp()
        elif self.__cur_state.command == self.Command.LOGOUT:
            self.__logout()
        else:
            print("Impossible state!")
            sys.exit(1)

        self.__cur_state = self.CurState()

    def run(self):
        while self.__cur_state.active:
            self.__print_cur_state()
            s = input(">>> ")
            self.__switch_cur_state(s)
            self.__send_request()


class AutoClient:
    def __init__(self, cfg: dict):
        pass


desc_str = """Client.

Client can work in two modes:
- manual mode, when You use command line prompt to interact with server
- automatic mode, when You only specify config file
"""


def parse_args() -> Mapping[str, Any]:
    parser = create_arguments_parser('client', desc_str)
    parser.add_argument('--mode', type=str,
                        help=r"overwrite mode from config (e.g.: auto)")

    return parse_args_as_dict(parser)


def main():
    args = parse_args()
    with open(args['config'], 'r') as f:
        cfg = yaml.safe_load(f)

    cfg['client']['mode'] = args.get('mode', cfg['client']['mode'])
    cfg['server']['host'] = args.get('host', cfg['server']['host'])
    cfg['server']['port'] = args.get('port', cfg['server']['port'])

    if cfg['client']['mode'] == 'manual':
        client = ManualClient(cfg)
    elif cfg['client']['mode'] == 'auto':
        client = AutoClient(cfg)
    else:
        return
    client.run()


if __name__ == "__main__":
    main()
