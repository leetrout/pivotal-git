#!/usr/bin/env python
import argparse
from collections import defaultdict
from commands import getoutput
import csv
import datetime
from os import environ
from os.path import expanduser
import pickle
import pprint
import re

from pivotal import Pivotal


PT_STORIES_PATH = expanduser('~/.pt_stories.pkl')
git_commit_hash = re.compile(r'/(\w{40})')
pt_branch = re.compile(r'.*?PT(\d+)')


def now():
    return datetime.datetime.utcnow()


def csv_rows_to_dict(rows):
    """Create dict from a list of rows."""
    result = {'stories': {}}
    iterations = defaultdict(lambda: [])
    max_iteration = 0
    
    # get indicies of data we want
    headers = rows.pop(0)
    id_idx = headers.index('Id')
    story_idx = headers.index('Story')
    iteration_idx = headers.index('Iteration')
    state_idx = headers.index('Current State')
    comment_idxs = []
    for idx, val in enumerate(headers):
        if val == 'Comment':
            comment_idxs.append(idx)

    for row in rows:
        comments = []
        commits = []
        row_len = len(row)
        for idx in comment_idxs:
            if idx < row_len:
                comment = row[idx]
                if comment:
                    comments.append(comment)
                    commits.extend(git_commit_hash.findall(comment))

        itr = row[iteration_idx]
        story = {
            'story': row[story_idx],
            'iteration': itr,
            'comments': comments,
            'commits': commits,
            'state': row[state_idx]
        }
        story_id = row[id_idx]
        result['stories'][story_id] = story
        if itr:
            max_iteration = max(max_iteration, int(itr))
        iterations[itr].append(row[id_idx])

    result['iterations'] = dict(iterations)
    result['max_iteration'] = str(max_iteration)
    result['updated'] = now()
    return result


def load_cache(csv_path, cache_path):
    cr = csv.reader(open(csv_path))
    stories = csv_rows_to_dict(list(cr))
    fh = open(cache_path, 'w')
    fh.write(pickle.dumps(stories))
    fh.close()


def clear_cache(cache_path):
    fh = open(cache_path, 'w')
    fh.write('')
    fh.close()


def update_cache(stories):
    print 'updating cache'
    stories['updated'] = now()
    fh = open(args.cache, 'w')
    fh.write(pickle.dumps(stories))
    fh.close()


def dump_cache(cache_path):
    pprint.pprint(get_cache(cache_path))


def get_cache(cache_path):
    fh = open(cache_path)
    cache = pickle.loads(fh.read())
    fh.close()
    return cache


def get_stories_from_pt():
    stories = {}
    pt = Pivotal(environ.get('PT_TOKEN'))
    etree = pt.projects(environ.get('PT_PROJECT_ID')).stories().get_etree()
    for story in etree.findall('story'):
        sid = story.findtext('id')
        comments = []
        commits = []
        for note in story.findall('notes/note'):
            comment = note.findtext('text')
            comments.append(comment)
            commits.extend(git_commit_hash.findall(comment))
        s = {
            'story': story.findtext('name'),
            'state': story.findtext('current_state'),
            'comments': comments,
            'commits': commits
        }
        stories[sid] = s
    return stories


def get_stories(skip_cache=False):
    if skip_cache:
        return get_stories_from_pt()
    try:
        return get_cache(args.cache)
    except EOFError:
        print "Couldn't load cache"
        return update_cache(get_stories_from_pt())


def git_branches():
    return [x.strip() for x in getoutput('git branch').split('\n')]


def pt_branches():
    branches = []
    for branch in git_branches():
        try:
            ptid = pt_branch.findall(branch)[0]
            branches.append((branch, ptid))
        except IndexError:
            continue
    return branches


def annotate_branch(branch, ptid=None, status=True, story=True):
    xtra = [branch]
    if ptid is None:
        try:
            ptid = pt_branch.findall(branch)[0]
        except IndexError:
            pass
    if ptid:
        s = get_stories().get(ptid, {})
        if status:
            xtra.append('[%s]' % s.get('state'))
        if story:
            xtra.append(s.get('story'))
    return ' '.join(xtra)


def annotate_branches():
    stories = get_stories()
    branches = git_branches()
    for idx, br in enumerate(branches):
        try:
            ptid = pt_branch.findall(br)[0]
            #branches[idx] = "%s [] (%s)" % (branches[idx], stories.get(ptid, {}).get('story'))
            branches[idx] = annotate_branch(branches[idx], ptid)
        except IndexError:
            continue
    return branches


def format_branch_output(branches):
    output = []
    for br in branches:
        if br.startswith('*'):
            output.append(" %s" % br)
        else:
            output.append("   %s" % br)
    return '\n'.join(output)


def not_merged(merge_branch):
    """Loop over pt branches and find commits not in branch."""
    unmerged = set()
    stories = get_stories()
    for branch, ptid in pt_branches():
        for commit in stories[ptid]['commits']:
            output = getoutput('git branch --contains %s' % commit)
            ab = annotate_branch(branch)
            if merge_branch not in output:
                unmerged.add(ab)
            if merge_branch in output:
                try:
                    unmerged.remove(ab)
                except KeyError:
                    pass
    return unmerged


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Pivotal Tracker Git[Hub] Tools")
    parser.add_argument('action', choices=[
        'info',
        'branches',
        'cache',
    ], help='specify the action to execute')

    # pivotal options
    # TODO finish pt args
    parser.add_argument('--token')

    # branch options
    parser.add_argument('--unmerged', action='store_true')
    parser.add_argument('--merge_branch', default='master')
    parser.add_argument('--status', action='store_true')

    # cache options
    parser.add_argument('--cache', dest='cache', default=PT_STORIES_PATH,
        help='cache path (defaults to %s)' % PT_STORIES_PATH)
    parser.add_argument('--load', dest='cache_load_path',
        help='path to csv file to populate cache')
    parser.add_argument('--dump', dest='cache_dump', action='store_true',
        help='dump the contents of the cache to stdout')
    parser.add_argument('--clear', dest='cache_clear', action='store_true',
        help='clear the cache')
    parser.add_argument('--skip', dest='skip_cache', action='store_true',
        help='skip the cache')

    args = parser.parse_args()
    action = args.action
    if action == 'info':
        print 'Caching to %s' % args.cache
    elif action == 'branches':
        if args.unmerged:
            print 'not merged to', args.merge_branch
            print format_branch_output(not_merged(args.merge_branch))
        else:
            get_stories(args.skip_cache)
            print format_branch_output(annotate_branches())
    elif action == 'cache':
        clp = args.cache_load_path
        clear = args.cache_clear
        dump = args.cache_dump
        if clp:
            print('Loading CSV into cache from %s' % clp)
            load_cache(clp, args.cache)
        elif dump:
            dump_cache(args.cache)
        elif clear:
            clear_cache(args.cache)
