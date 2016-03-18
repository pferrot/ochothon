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
import json
import logging
import os
import shutil
import tempfile
import time
import yaml

from ochopod.core.fsm import diagnostic
from ochopod.core.utils import merge
from ochopod.core.utils import retry
from random import choice
from subprocess import Popen, PIPE
from threading import Thread
from toolset.io import fire, run
from toolset.tool import Template
from yaml import YAMLError

#: Our ochopod logger.
logger = logging.getLogger('ochopod')


class _Automation(Thread):

    def __init__(self, proxy, template, overrides, namespace, release, suffix, timeout, strict, kill_first, rolling, wait):
        super(_Automation, self).__init__()

        self.namespace = namespace
        self.out = \
            {
                'ok': False,
                'up': [],
                'down': []
            }
        self.overrides = overrides
        self.proxy = proxy
        self.release = release
        self.suffix = suffix
        self.strict = strict
        self.kill_first = kill_first
        self.template = template
        self.timeout = max(timeout, 5)
        self.rolling = rolling
        self.wait = max(wait, 0)

        self.start()

    def run(self):
        try:

            #
            # - we need to pass the framework master IPs around (ugly)
            #
            assert 'MARATHON_MASTER' in os.environ, '$MARATHON_MASTER not specified (check your portal pod)'
            master = choice(os.environ['MARATHON_MASTER'].split(','))
            headers = \
                {
                    'content-type': 'application/json',
                    'accept': 'application/json'
                }

            with open(self.template, 'r') as f:

                #
                # - parse the template yaml file (e.g container definition)
                #
                raw = yaml.load(f)
                assert raw, 'empty YAML input (user error ?)'

                #
                # - merge with our defaults
                # - we want at least the cluster & image settings
                # - TCP 8080 is added by default to the port list
                #
                defaults = \
                    {
                        'start': True,
                        'debug': False,
                        'settings': {},
                        'ports': [8080],
                        'verbatim': {}
                    }

                cfg = merge(defaults, raw)
                assert 'cluster' in cfg, 'cluster identifier undefined (user error ?)'
                assert 'image' in cfg, 'docker image undefined (user error ?)'

                #
                # - if a suffix is specified append it to the cluster identifier
                #
                if self.suffix:
                    cfg['cluster'] = '%s-%s' % (cfg['cluster'], self.suffix)

                qualified = '%s.%s' % (self.namespace, cfg['cluster'])

                # grep existing pods
                def _query_existing(zk):
                    replies = fire(zk, qualified, 'info')
                    return len(replies), [[key, seq]
                                          for key, (seq, _, code) in sorted(replies.items()) if code == 200]

                total, js = run(self.proxy, _query_existing)

                if total == 0:
                    self.out['up'] = []
                    self.out['down'] = []
                    self.out['ok'] = False
                    logger.info('%s : no pod to update ' % self.template)
                else:
                    env = os.environ
                    hints = json.loads(env['ochopod'])
                    env['OCHOPOD_ZK'] = hints['zk']


                    def _run_command(command, need_template = False):
                        #logger.debug("Running command: %s" % command)
                        try:
                            out = []
                            tmp = tempfile.mkdtemp()
                            if need_template:
                                shutil.copy(self.template, tmp)
                            pid = Popen('toolset %s -j' % command, shell=True, stdout=PIPE, stderr=None, env=env, cwd=tmp)

                            while 1:
                                code = pid.poll()
                                line = pid.stdout.readline()
                                if not line and code is not None:
                                    break
                                elif line:
                                    out += [line.rstrip('\n')]

                            #logger.debug("Command output: %s" % ' '.join(out))
                            ok = pid.returncode == 0
                            return ok, json.loads(' '.join(out))
                        finally:
                            #
                            # - make sure to cleanup our temporary directory
                            #
                            shutil.rmtree(tmp)


                    def _deploy_command(nb_pods):
                        return _run_command('deploy %s -n %s -t %d -p %d %s' % (self.template, self.namespace, self.timeout, nb_pods, "--strict" if self.strict else ""), True)

                    def _kill_command(seq):
                        return _run_command('kill %s -t %d -i %s' % (qualified, self.timeout, ' '.join(str(x) for x in seq)))

                    def _scale_command(seq, nb_pods):
                        # We do not need a long timeout as we check things ourselves anyway.
                        scale_result, scale_output = _run_command('scale %s -g %d -f @%d -t %d' % (qualified, seq, nb_pods, 5))
                        #logger.debug("Scale result: %s" % str(scale_result))
                        #logger.debug("Scale output: %s" % str(scale_output))
                        return scale_result, scale_output

                    def _grep_command():
                        return _run_command('grep %s' % qualified)

                    def _diff(a, b):
                        b = set(b)
                        return [aa for aa in a if aa not in b]



                    def _kill_deploy(seq, counter = 0):
                        ok = False
                        pod_log = "%s #%s" % (qualified, ' #'.join(str(x) for x in seq))
                        nb_pods_log = "%d pod%s" % (len(seq), "" if len(seq) <= 1 else "s")

                        def _scale():
                            # Sale and then retrieve new pod index (it is not returned by the scale command).
                            scale_result, _ = _scale_command(self.out['up'][0], counter + 1)
                            #_, new_js = run(self.proxy, _query_existing)
                            seq_orig = [i[1] for i in js]
                            seq_added_already = self.out['up']
                            #seq_new = [i[1] for i in new_js]
                            
                            # It might take some time until ne new pod is available through grep command.
                            @retry(timeout=60, pause=5)
                            def _get_new_pod_seq(): 
                                #seq_new = _grep_seq()
                                _, js_new = run(self.proxy, _query_existing, 10)
                                seq_new = [i[1] for i in js_new]                   
                                
                                seq_diff = _diff(seq_new, seq_orig + seq_added_already)
                                assert len(seq_diff) == 1, 'exactly one pod should have been added'
                                return seq_diff[0]
                                
                                
                            new_pod_seq = _get_new_pod_seq()
                            
                            self.out['up'].append(new_pod_seq)
                            if scale_result:
                                logger.debug("Replacement pod for %s successfully created (%s), new one(s): #%d" % (pod_log, nb_pods_log, new_pod_seq))
                                return True
                            else:
                                logger.info("Failed to scale up cluster to create replacement pod for %s" % pod_log)
                                return False

                        def _kill():
                            kill_result, kill_out = _kill_command(seq)
                            self.out['down'].extend(kill_out[qualified]['down'])
                            if kill_result:
                                logger.debug("%s successfully killed (%s)" % (pod_log, nb_pods_log))
                                return True
                            else:
                                logger.info("Failed to kill %s" % pod_log)
                                return False

                        def _deploy():
                            deploy_result, deploy_out = _deploy_command(len(seq))
                            self.out['up'].extend(deploy_out[self.template]['up'])
                            if deploy_result:
                                logger.debug("Replacement pod(s) for %s successfully deployed (%s), new one(s): #%s" % (pod_log, nb_pods_log, ' #'.join(str(x) for x in deploy_out[self.template]['up'])))
                                return True
                            else:
                                logger.info("Failed to deploy replacement pod(s) for %s" % pod_log)
                                return False

                        def _grep_seq():
                            grep_result, grep_output = _grep_command()
                            #logger.debug("Grep output: %s" % str(grep_output))
                            assert grep_result, "failed to grep container"
                            seq = []
                            for one_key in grep_output.keys():
                                seq.append(int(one_key[one_key.rfind('#') + 1:]))
                            #logger.debug("Grep seq: %s" % ', '.join(str(x) for x in seq))
                            return seq


                        def _deploy_or_scale():
                            if len(self.out['up']) == 0:
                                return _deploy()
                            else:
                                return _scale()

                        if self.kill_first:
                            if _kill():
                                ok = _deploy_or_scale()
                            else:
                                ok = False
                        else:
                            if _deploy_or_scale():
                                ok = _kill()
                            else:
                                ok = False

                        return ok

                    if self.rolling:
                        logger.debug("Rolling update: ON")
                        ok_rolling = True
                        for counter, (_, seq) in enumerate(js):
                            status = _kill_deploy([seq], counter)
                            ok_rolling = ok_rolling and status
                            if self.wait > 0 and counter < len(js) - 1:
                                logger.debug("Now waiting for %d seconds" % self.wait)
                                time.sleep(self.wait)
                                logger.debug("Done waiting")
                        self.out['ok'] = ok_rolling

                    # No rolling update
                    else:
                        logger.debug("Rolling update: OFF")
                        self.out['ok'] = _kill_deploy([i[1] for i in js])

                    #logger.debug("ok: %s" % str(self.out['ok']))
                    #logger.debug("up: %s" % ', '.join(str(x) for x in self.out['up']))
                    #logger.debug("down: %s" % ', '.join(str(x) for x in self.out['down']))


        except AssertionError as failure:

            logger.debug('%s : failed to update -> %s' % (self.template, failure))

        except YAMLError as failure:

            if hasattr(failure, 'problem_mark'):
                mark = failure.problem_mark
                logger.debug('%s : invalid deploy.yml (line %s, column %s)' % (self.template, mark.line+1, mark.column+1))

        except Exception as failure:

            logger.debug('%s : failed to update -> %s' % (self.template, diagnostic(failure)))

    def join(self, timeout=None):

        Thread.join(self)
        return self.out


def go():

    class _Tool(Template):

        help = \
            '''
                Updates a cluster by replacing all of the pods in that cluster with. The existing cluster is determined by the cluster name in the YAML definition together
                with the specified namespace. If the cluster does not exist, no action is taken. If a cluster with the appropriate name exists, then all of its pods are killed
                and replaced by new ones that are deployed using the provided YAML definition. I.e. the cluster size does not change after the update is completed.

                Rolling updates (i.e. pods are replaced one by one) are supported via the --rolling switch. With rolling updates, it is also possible
                to specify a wait time between pod updates (see the -w parameter).

                By default, new pods are deployed before old ones are killed. It is possible to kill old pods first vie the --kill_first switch.

                This tool supports optional output in JSON format for 3rd-party integration via the -j switch.
            '''

        tag = 'update'

        def customize(self, parser):

            parser.add_argument('containers', type=str, nargs='+', help='1+ YAML definitions (e.g marathon.yml)')
            parser.add_argument('-j', action='store_true', dest='json', help='json output')
            parser.add_argument('-n', action='store', dest='namespace', type=str, default='marathon', help='namespace')
            parser.add_argument('-o', action='store', dest='overrides', type=str, nargs='+', help='overrides YAML file(s)')
            parser.add_argument('-r', action='store', dest='release', type=str, help='docker image release tag')
            parser.add_argument('-s', action='store', dest='suffix', type=str, help='optional cluster suffix')
            parser.add_argument('-t', action='store', dest='timeout', type=int, default=60, help='timeout in seconds')
            parser.add_argument('-w', action='store', dest='wait', type=int, default=0, help='when doing rolling updates, time in seconds to wait between updating pods (default to 0)')
            parser.add_argument('--kill_first', action='store_true', dest='kill_first', help='kills the pod before deploying the new one (default is to first deploy then kill the old pod)')
            parser.add_argument('--rolling', action='store_true', dest='rolling', help='updates pods one by one')
            parser.add_argument('--strict', action='store_true', dest='strict', help='waits until all pods are running')

        def body(self, args, _, proxy):

            assert len(args.containers), 'at least one container definition is required'

            #
            # - load the overrides from yaml if specified
            #
            overrides = {}
            if not args.overrides:
                args.overrides = []

            for path in args.overrides:
                try:
                    with open(path, 'r') as f:
                        overrides.update(yaml.load(f))

                except IOError:

                    logger.debug('unable to load %s' % args.overrides)

                except YAMLError as failure:

                    if hasattr(failure, 'problem_mark'):
                        mark = failure.problem_mark
                        assert 0, '%s is invalid (line %s, column %s)' % (args.overrides, mark.line+1, mark.column+1)

            #
            # - run the workflow proper (one thread per container definition)
            #
            threads = {template: _Automation(
                proxy,
                template,
                overrides,
                args.namespace,
                args.release,
                args.suffix,
                args.timeout,
                args.strict,
                args.kill_first,
                args.rolling,
                args.wait) for template in args.containers}

            #
            # - wait for all our threads to join
            #
            n = len(threads)
            outcome = {key: thread.join() for key, thread in threads.items()}
            pct = (100 * sum(1 for _, js in outcome.items() if js['ok'])) / n if n else 0
            up = sum(len(js['up']) for _, js in outcome.items())
            down = sum(len(js['down']) for _, js in outcome.items())
            logger.info(json.dumps(outcome) if args.json else '%d%% success (+%d pods, -%d pods)' % (pct, up, down))
            return 0 if pct == 100 else 1

    return _Tool()
