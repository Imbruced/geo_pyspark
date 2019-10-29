# -*- coding: utf-8 -*-

#
# TODO: extract in a Sphinx plugin
#

import git
import os
import os.path
import time

from sphinx.util import logging
logger = logging.getLogger(__name__)

# Create a GIT repository object if one does exist in '/doc'
documentation_directory = '/doc'

if os.path.isdir(os.path.join(documentation_directory, '.git')):
    git_repository = git.Repo(documentation_directory)
    logger.info("Git repository found in '%s' directory" % documentation_directory)
else:
    logger.warning("Git repository not found in '%s'. Skipping..." % documentation_directory)
    git_repository = None

# Proceed only if there is a Git repository
if git_repository:

    head_commit = git_repository.head.commit

    # Check if the current HEAD has been tagged
    head_tag = next((tag for tag in git_repository.tags if tag.commit == head_commit), None)

    # Check if all changes have been committed
    all_committed = True

    if len(git_repository.index.diff(head_commit)) > 0:
        logger.info("Using commit hash because there are differences between the index and the commit’s tree your HEAD points to")
        all_committed = False

    if len(git_repository.index.diff(None)) > 0:
        logger.info("Using commit hash because there are differences between the index and the working tree")
        all_committed = False

    if len(git_repository.untracked_files) > 0:
        logger.info("Using commit hash because there are untracked files")
        all_committed = False

    # Configure
    if all_committed:

        if head_tag:
            commit_version = str(head_tag)
        else:
            commit_version = 'Commit ' + head_commit.hexsha[:8]

        commit_hash = head_commit.hexsha
        commit_date = time.gmtime(head_commit.committed_date)
        commit_date_str = time.strftime("%Y/%m/%d %H:%M:%S", commit_date)

        # html_context['commit'] = commit_date_str + ' ' + commit_hash
        html_context['last_updated'] = commit_date_str + ' Commit ' + commit_hash
        version = commit_version
        release = commit_version

    else:
        current_time = time.strftime("%Y/%m/%d %H:%M:%S", time.gmtime(time.time()))
        html_context['last_updated'] = current_time + " [WORKING VERSION]"
        version = current_time + "<br>[WORKING VERSION]"
        release = current_time + "<br>[WORKING VERSION]"
