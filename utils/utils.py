import argparse
import logging
from logging import handlers
from typing import Mapping, Any


def get_logger(cfg: dict, name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    if cfg['logs'].get('con', False):
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    file = cfg['logs'].get('file', '')
    if file != '':
        fh = logging.FileHandler(file)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    server = cfg['logs'].get('server')
    if server is not None:
        hh = handlers.HTTPHandler(host='{}:{}'.format(server.get('host', '0.0.0.0'), server.get('port', 20000)),
                                  url='/api/v1/add_log', method='POST')
        hh.setFormatter(formatter)
        logger.addHandler(hh)

    return logger


def create_arguments_parser(prog: str, desc_str: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog=prog, formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=desc_str)
    parser.add_argument('-v', '--version', action='version', version='%(prog)s v0.1')

    parser.add_argument('-c', '--config', type=str, default='./config.yml',
                        help=r"path to {} config (default: %(default)s)".format(prog))

    parser.add_argument('--host', type=str,
                        help=r"overwrite host from config (e.g.: 172.12.0.101)")
    parser.add_argument('--port', type=int,
                        help=r"overwrite port from config (e.g.: 443)")

    return parser


def parse_args_as_dict(parser: argparse.ArgumentParser) -> Mapping[str, Any]:
    args = vars(parser.parse_args())
    return {k: v for k, v in args.items() if v is not None}
