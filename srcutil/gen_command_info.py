#!/usr/bin/env python
import json
import argparse
import os.path


class Scope:
    def __init__(self, start, end, file):
        self.file = file
        self.start = start
        self.end = end

    def __enter__(self):
        self.write(self.start + '{\n')
        Scope.indent += 1
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        Scope.indent -= 1
        self.write(f'}}{self.end}\n')

    def write(self, line):
        indent = '  ' * Scope.indent
        self.file.write(indent + line)

Scope.indent = 0

def get_function_signature(name):
    tokens = [token.title() for token in name.replace('.', ' ').split(' ')]
    value = ''.join(tokens)
    return f'int Set{value}Info(RedisModuleCtx *ctx, RedisModuleCommand *cmd)'

def generate_header_file(file_name, command_names):
    license = '/*\n* Copyright Redis Ltd. 2016 - present\n'\
              '* Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or'\
              '\n* the Server Side Public License v1 (SSPLv1).\n*/\n'
    with open(file_name + '.h', 'w') as header:
        header.write(license)
        header.write('\n// This file is generated by gen_command_info.py\n')
        header.write('#pragma once\n')
        header.write('#include "redismodule.h"\n\n')
        for name in command_names:
            header.write(f'{get_function_signature(name)};\n')

def generate_history(file, changes):
    with Scope('.history = (RedisModuleCommandHistoryEntry[])', ',', file) as history:
        for change in changes:
            version, what = change
            history.write(f'{{"{version}", "{what}"}},\n')
        history.write('{0}\n')

def generate_arguments(file, member, arguments):
    min_arity = 0
    with Scope(f'.{member} = (RedisModuleCommandArg[])', ',', file) as args_scope:
        for arg in arguments:
            with Scope('', ',', file) as arg_scope:
                for string_arg in ['name', 'token', 'summary', 'since', 'deprecated_since', 'display_text']:
                    if string_arg in arg:
                        arg_scope.write(f'.{string_arg} = "{arg[string_arg]}",\n')
                if 'type' in arg:
                    type_text = arg['type'].replace('-', '_').upper()
                    arg_scope.write(f'.type = REDISMODULE_ARG_TYPE_{type_text},\n')
                flags = []
                if 'optional' not in arg:
                    min_arity += 1
                for flag in ['optional', 'multiple', 'multiple-token']:
                    if flag in arg:
                        flag_text = flag.replace('-', '_').upper()
                        flags.append(f'REDISMODULE_CMD_ARG_{flag_text}')
                if len(flags) > 0:
                    flags_text = ' | '.join(flags)
                    arg_scope.write(f'.flags = {flags_text},\n')
                if 'arguments' in arg:
                    generate_arguments(file, 'subargs', arg['arguments'])
        args_scope.write('{0}\n')
    return min_arity

def generate_redis_module_command_info(cmd_info, file):
    with Scope('const RedisModuleCommandInfo info = ', ';', file) as info:
        info.write('.version = REDISMODULE_COMMAND_INFO_VERSION,\n')
        for key, value in cmd_info.items():
            if key in ['group']:
                continue
            if type(value) == str:
                info.write(f'.{key} = "{value}",\n')
            elif key == 'history':
                generate_history(file, value)
            elif key == 'arguments':
                min_arity = generate_arguments(file, 'args', value)
                info.write(f'.arity = -{min_arity},\n')

def generate_command_info_definition(name, info, file):
    signature = get_function_signature(name)
    file.write(f'// Info for {name}\n')
    with Scope(signature + ' ', '\n', file) as function:
        generate_redis_module_command_info(info, file)
        # If RedisModule_SetCommandInfo is not available, quietly return REDISMODULE_OK
        with Scope('if (RedisModule_SetCommandInfo == NULL)', '', file) as no_set_command_info:
            no_set_command_info.write('return REDISMODULE_OK;\n')
        function.write('return RedisModule_SetCommandInfo(cmd, &info);\n')


def generate_c_file(file_name, commands):
    with open(file_name + '.c', 'w') as c_file:
        c_file.write('// This file is generated by gen_command_info.py\n')
        c_file.write('#include "redismodule.h"\n')
        c_file.write(f'#include "{os.path.basename(file_name)}.h"\n\n')
        for name, info in commands.items():
            generate_command_info_definition(name, info, c_file)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-j', '--json', help='JSON commands input file', required=True)
    parser.add_argument('-f', '--file', help='Output file name, will output .h and .c files',
                        default='command_info', required=True)
    args = parser.parse_args()
    with open(args.json, 'r') as f:
        data = json.load(f)
        generate_header_file(args.file, data.keys())
        generate_c_file(args.file, data)


if __name__ == '__main__':
    main()