"""
Example script built on top of this framework. Included for testing purposes.
"""

import argparse
import copy
import pexpect
import multiprocessing
import numpy
import random
import time

from common import *


NUM_STATES = 50
NUM_ACTIONS = 5
STATES = list(range(NUM_STATES))
ACTIONS = list(range(NUM_ACTIONS))
REWARDS = {s: round(random.uniform(-1, 1), 5) for s in STATES}
MODEL = {(s, a): [[]] for s in STATES for a in ACTIONS}
for s in STATES:
    for a in ACTIONS:
        s_a = MODEL[(s, a)][0]
        next_s = random.choice(STATES)
        r = REWARDS[next_s]
        s_a.append([next_s, r])
        for _ in range(len(ACTIONS) - 1):
            next_s = random.choice(STATES)
            r = random.choice([None, REWARDS[next_s]])
            s_r = (next_s, r)
            if r is not None and s_r not in s_a:
                s_a.append(s_r)
        probs = [float(random.randint(1, 100)) for _ in s_a]
        total_prob = sum(probs)
        for i in range(len(probs)):
            probs[i] = probs[i] / total_prob
        MODEL[(s, a)].append(probs)
GAMMA = 0.9
MAX_ITER = 10


def num():
    return int(pexpect.run('curl \
            http://metadata/computeMetadata/v1beta1/instance/attributes/num'))


def update_model(model):
    global MODEL
    MODEL = model


def base(s):
    return random.choice(ACTIONS)

base = {s: base(s) for s in STATES}


def simulate(s, a):
    transitions, probs = MODEL[(s, a)]
    choice = random.uniform(0, 1)
    i = 0
    while choice > probs[i]:
        choice -= probs[i]
        i += 1
    return transitions[i]


def rollout(s, a, policy, len_traj):
    next_s, r = simulate(s, a)
    emp_Q = r
    s = next_s
    for t in range(len_traj):
        next_s, r = simulate(s, policy[s])
        emp_Q += (GAMMA ** t) * r
        s = next_s
    return emp_Q


def rollout_map(args):
    return args[0], args[1], rollout(*args)


def val_iter():
    V = {s: 0 for s in STATES}
    epsilon = 1e-300
    while True:
        delta = 0
        for s in STATES:
            val = V[s]
            possible_values = []
            for a in ACTIONS:
                val_a = 0
                transitions, probs = MODEL[(s, a)]
                for (next_s, r), p in zip(transitions, probs):
                    val_a += p * (r + GAMMA * V[next_s])
                possible_values.append(val_a)
            V[s] = max(possible_values)
            delta = max(delta, abs(val - V[s]))
        if delta < epsilon:
            break
    def optimal_policy(s):
        values = {}
        for a in ACTIONS:
            val_a = 0
            transitions, probs = MODEL[(s, a)]
            for (next_s, r), p in zip(transitions, probs):
                val_a += p * (r + GAMMA * V[next_s])
            values[a] = val_a
        return max(values, key=lambda x: values[x])
    return optimal_policy


def evaluate_approx(policy):
    V = [0 for s in STATES]
    epsilon = 1e-200
    while True:
        delta = 0
        old_V = V[:]
        for s in STATES:
            val = V[s]
            V[s] = 0
            transitions, probs = MODEL[(s, policy[s])]
            for (next_s, r), p in zip(transitions, probs):
                V[s] += p * (r + GAMMA * old_V[next_s])
            delta = max(delta, abs(val - V[s]))
        if delta < epsilon:
            return V


def evaluate_exact(policy):
    prob_mat = []
    for s in STATES:
        prob_s = [0 for _ in STATES]
        transitions, probs = MODEL[(s, policy[s])]
        for (next_s, r), p in zip(transitions, probs):
            prob_s[next_s] = p
        prob_mat.append(prob_s)
    P = numpy.matrix(prob_mat)
    R = numpy.matrix([REWARDS[s] for s in STATES]).T
    return list(float(mat) for mat in (numpy.identity(NUM_STATES) - GAMMA * P).I * R)


def pol_iter(eval_func=evaluate_exact):
    policy = copy.copy(base)
    while True:
        V = eval_func(policy)
        diff_a = 0
        for s in STATES:
            old_a = policy[s]
            values = {}
            for a in ACTIONS:
                val_a = 0
                transitions, probs = MODEL[(s, a)]
                for (next_s, r), p in zip(transitions, probs):
                    val_a += p * (r + GAMMA * V[next_s])
                values[a] = val_a
            policy[s] = max(values, key=lambda x: values[x])
            if old_a != policy[s]:
                diff_a += 1
        if diff_a > 0:
            break
    return policy


def approx_optimal(policy, optimal_perf, i):
    tolerance = 1
    num_diff = 0
    for s in STATES:
        if policy[s] != optimal[s]:
            num_diff += 1
            if num_diff > tolerance:
                print('policy #{} is not good enough'.format(i))
                return False
    print('policy #{} is good enough'.format(i))
    print('num diff: {}'.format(num_diff))
    return True


def learn(training_set, last_policy):
    cache = copy.deepcopy(training_set)
    def new_policy(s):
        if s in cache:
            return cache[s]
        return last_policy[s]
    new_policy = {s: new_policy(s) for s in STATES}
    num_diff, num_nopt = 0, 0
    for s in STATES:
        if new_policy[s] != last_policy[s]:
            num_diff += 1
        if new_policy[s] != optimal[s]:
            num_nopt += 1
    wf(colorize('{}, {}\t'.format(num_diff, num_nopt), 'yellow'))
    return new_policy


def rcpi(num_traj, len_traj, par=False):
    policy = base
    i = 1
    training_set = copy.copy(base)
    while policy != optimal and i <= MAX_ITER:
        start = time.time()
        rollout_args = [(s, a, policy, len_traj) \
                    for _ in range(num_traj) for a in ACTIONS for s in STATES]
        if par:
            total_emp_Q = parallel.parallel_map(rollout_map, rollout_args)
        else:
            pool = multiprocessing.Pool()
            total_emp_Q = pool.map(rollout_map, rollout_args)
            pool.close()
            pool.join()
        for s in STATES:
            emp_Qs = {}
            total_emp_Qs = [e[1:] for e in total_emp_Q if e[0] == s]
            for a in ACTIONS:
                emp_Qs[a] = sum(e[1] for e in total_emp_Qs if e[0] == a) / num_traj
            best_a = max(emp_Qs, key=lambda x: emp_Qs[x])
            training_set[s] = best_a
        policy = learn(training_set, policy)
        print('running time of iter #{}:\t{}'.format(i, time.time() - start))
        i += 1
    return policy


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--parallel', action='store_true')
    parser.add_argument('-c', '--cluster', type=str, \
            help='The cluster that will be used for this script')
    parser.add_argument('-s', '--slave', action='store_true')
    parser.add_argument('-v', '--value', action='store_true')
    parser.add_argument('-r', '--rollouts', type=int)
    args = parser.parse_args()
    val_iter_pol = val_iter()
    opt_val = {s: val_iter_pol(s) for s in STATES}
    opt_app = pol_iter(eval_func=evaluate_approx)
    if args.value:
        optimal = opt_val
    else:
        optimal = pol_iter()
    num_traj = args.rollouts or 100
    if args.parallel:
        if not args.slave:
            import parallel
            parallel.claim_cluster(args.cluster)
            parallel.apply_on_all_insts(update_model, (MODEL,))
            best = rcpi(num_traj, 100, par=True)
        else:
            import slave
            print('starting slave loop')
            slave.run_slave_loop()
    else:
        best = rcpi(num_traj, 100)

