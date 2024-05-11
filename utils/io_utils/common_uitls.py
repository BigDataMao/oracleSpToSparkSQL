# -*- coding: utf-8 -*-

def to_color_str(text, color):
    """
    将文本转换为彩色文本
    :param text: 文本
    :param color: 颜色,比如 'red', 'green' ...
    :return: text: 彩色文本
    """
    colors = {
        'black': '\033[30m',
        'red': '\033[31m',
        'green': '\033[32m',
        'yellow': '\033[33m',
        'blue': '\033[34m',
        'magenta': '\033[35m',
        'cyan': '\033[36m',
        'white': '\033[37m',
        'reset': '\033[0m'
    }

    if color.lower() in colors:
        return '{}{}{}'.format(colors[color.lower()], text, colors['reset'])
    else:
        return text
