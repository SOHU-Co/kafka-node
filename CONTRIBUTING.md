# How to contribute

All patches or feature evolutions are welcome.

## Getting Started

* Make sure you have a [GitHub account](https://github.com/signup/free)
* Submit a ticket for your issue, assuming one does not already exist.
  * Clearly describe the issue including steps to reproduce when it is a bug.
  * Make sure you fill in the earliest version that you know has the issue.
* Fork the repository on GitHub

## Making Changes

* Create a topic branch from where you want to base your work 
(This is usually the master branch on your forked project).
* Make commits of logical units.
* Check for unnecessary whitespace with `git diff --check` before committing.
* The code style of current code base should be preserved
* Make sure you have added the necessary tests for your changes, specially if
you added a new feature.
* Run _all_ the tests to assure nothing else was accidentally broken.

## Submitting Changes

* Push your changes to a topic branch in your fork of the repository.
* Submit a pull request to the repository.
* Make sure that the PR has a clean log message and don't hesitate to squash 
and rebase your commits in order to preserve a clean history log.

## Code reviewers

* For small fixes, one can merge PR directly.
* For new features or big change of current code base, at least two 
collaborators should LGTM before merging.
* Rebase instead of merge to avoid those "Merge ...." commits, is recommended 
(see https://github.com/blog/2141-squash-your-commits)