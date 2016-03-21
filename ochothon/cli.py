#! /usr/bin/env python
#
# Copyright (c) 2015 Autodesk Inc.
# All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
Minimalistic self-contained wrapper performing the curl calls to the ochopod proxy. The input is
turned into a POST -H X-Shell:<> to the proxy at port TCP 9000 (default). Any token from that input that
matches a local file (wherever the script is running from) will force an upload for the said file.
This mechanism is used for instance to upload the container definition YAML files when deploying a
new cluster.

The proxy ip or hostname is either passed as the first command-line argument or via $OCHOPOD_PROXY.
The default TCP port (9000) can be overridden with the syntax <prox_ip>:<proxy_port>. 
Type "help" to get the list of supported commands.

For instance:

 $ ocho cli my-cluster
 welcome to the ocho CLI ! (CTRL-C or exit to get out)
 my-cluster > help
 available commands -> bump, deploy, grep, kill, log, ls, nodes, off, on, ping, poll, port

You can also run a one-shot command. For instance if you just need to list all your pods:

 $ ocho cli my-cluster ls
 3 pods, 100% replies ->

 cluster                                        |  ok   |  status
                                                |       |
 marathon.portal                                |  1/1  |
 test.web-server                                |  2/2  |
"""

import cmd
import hashlib
import hmac
import json
import os
import tempfile
import shutil

from common import shell
from os.path import abspath, basename, expanduser, isdir, isfile, join
from requests import post
from sys import exit

PORT_DEFAULT = 9000

def cli(args):

    tmp = tempfile.mkdtemp()
    try:

        class Shell(cmd.Cmd):

            def __init__(self, ipAndPort, token=None):
                cmd.Cmd.__init__(self)
                #
                # - do not show port number when the default one is used
                #
                self.prompt = '%s > ' % (ipAndPort if not ipAndPort.endswith(':%s' % PORT_DEFAULT) else ipAndPort[:-len(':%s' % PORT_DEFAULT)])
                self.ruler = '-'
                self.token = token

            def precmd(self, line):
                return 'shell %s' % line if line not in ['exit'] else line

            def emptyline(self):
                pass

            def do_exit(self, _):
                raise KeyboardInterrupt

            def do_shell(self, line):
                if line:
                    tokens = line.split(' ')

                    #
                    # - update from @stphung -> reformat the input line to handle indirect paths transparently
                    # - for instance ../foo.bar will become foo.bar with the actual file included in the multi-part post
                    #
                    files = {}
                    substituted = tokens[:1]
                    for token in tokens[1:]:
                        expanded = expanduser(token)
                        full = abspath(expanded)
                        tag = basename(full)
                        if isfile(expanded):

                            #
                            # - if the token maps to a local file upload it
                            # - this is for instance what happens when you do 'deploy foo.yml'
                            #
                            files[tag] = abspath(full)
                            substituted += [tag]

                        elif isdir(expanded):

                            #
                            # - if the token maps to a local directory TGZ & upload it
                            # - this is typically used to upload settings & script for our CD pipeline
                            # - the TGZ is stored in our temp. directory
                            #
                            path = join(tmp, '%s.tgz' % tag)
                            shell('tar zcf %s *' % path, cwd=full)
                            files['%s.tgz' % tag] = path
                            substituted += ['%s.tgz' % tag]

                        else:
                            substituted += [token]

                    #
                    # - compute the SHA1 signature if we have a token
                    # - prep the CURL statement and run it
                    # - we should always get a HTTP 200 back with some UTF-8 json payload
                    # - parse & print
                    #
                    line = ' '.join(substituted)
                    digest = 'sha1=' + hmac.new(self.token, line, hashlib.sha1).hexdigest() if self.token else ''
                    url = 'http://%s/shell' % ipAndPort
                    headers = \
                        {
                            'X-Signature': digest,
                            'X-Shell': line
                        }                    
                    files_post = {file_id: open(files[file_id], 'rb') for file_id in files.keys()}
                    reply = post(url, headers=headers, files=files_post)
                    code = reply.status_code
                    out = reply.content                    
                    js = json.loads(out.decode('utf-8'))
                    print(js['out'] if code is 200 else 'i/o failure (is the proxy down ?)')

        #
        # - partition ip and args by looking for OCHOPOD_PROXY first
        # - if OCHOPOD_PROXY is not used, treat the first argument as the ip
        #
        ipAndPort = None
        if 'OCHOPOD_PROXY' in os.environ:
            ipAndPort = os.environ['OCHOPOD_PROXY']
        elif len(args):
            ipAndPort = args[0]
            args = args[1:] if len(args) > 1 else []

        #
        # - fail if left undefined
        #
        assert ipAndPort is not None, 'either set $OCHOPOD_PROXY or pass the proxy IP as an argument'

        #
        # - 9000 is the default port
        #
        if ":" not in ipAndPort:
            ipAndPort = ipAndPort + (':%s' % PORT_DEFAULT)

        #
        # - set the secret token if specified via the $OCHOPOD_TOKEN variable
        # - if not defined or set to an empty string the SHA1 signature will not be performed
        #
        token = os.environ['OCHOPOD_TOKEN'] if 'OCHOPOD_TOKEN' in os.environ else None

        #
        # - determine whether to run in interactive or non-interactive mode
        #
        if len(args):
            command = " ".join(args)
            Shell(ipAndPort, token).do_shell(command)
        else:
            print('welcome to the ocho CLI ! (CTRL-C or exit to get out)')
            if token is None:
                print 'warning, $OCHOPOD_TOKEN is undefined'
            Shell(ipAndPort, token).cmdloop()

    except KeyboardInterrupt:
        exit(0)

    except Exception as failure:
        print('internal failure <- %s' % str(failure))
        exit(1)

    finally:
        shutil.rmtree(tmp)
