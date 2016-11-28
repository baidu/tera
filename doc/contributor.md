# How to contribute

Tera is BSD 3-Clause licensed and accepts contributions via GitHub pull requests. 
This document outlines some of the conventions on commit message formatting, 
bug reporting and other resources to make getting your contribution 
into Tera easier.

## Email and chat

- Email: tera_dev at baidu.com
- IRC: QQ group 340418305

## Getting started

- __Star__ Tera project
- __Fork__ the repository on GitHub
- Read the __[BUILD](../BUILD)__ for build instructions

## Reporting bugs and creating issues

Reporting bugs is one of the best ways to contribute. If any part of the Tera project 
has bugs or documentation mistakes, please let us know by __opening an issue__. We 
treat bugs and mistakes very seriously and believe no issue is too small. Before creating 
a bug report, please check that an issue reporting the same problem does not already exist.

To make the bug report accurate and easy to understand, please try to write the bug reports that are:

- Specific. Include as much details as possible: which version, what environment, 
  what configuration, etc. If the bug is related to running the Tera service, 
  please attach the Tera log and runtime stack.

- Reproducible. Include the steps to reproduce the problem.

- Unique. Please do not duplicate existing bug report.

- Scoped. One bug per report. Do not follow up with another bug inside one report.

## Contribution flow

This is a rough outline of what a contributor's workflow looks like:

- Create a topic branch from where you want to base your work. This is usually master.
- Make commits of logical units and __must__ add test case for bug fixing or add new functionality.
- Make sure your commit messages are in the proper format (see below).
- Push your changes to a topic branch in your fork of the repository.
- Submit a pull request to baidu/tera.
- Your PR must receive a LGTM from __two__ committers.

Thanks for your contributions!

### Code style

Tera follows Google's C++ coding style except for 4 spaces of default indentation. See the [style doc](https://google.github.io/styleguide/cppguide.html) for details.

Please follow this style to make Tera easy to review, maintain and develop.

### Commit message format

We follow a rough convention for commit messages that is designed to answer two
questions: what changed and why. The subject line should feature the what and
the body of the commit should describe the why.

```
issue=#1000: support leveldb's log for switching and evicting 

tera use leveldb's PosixLogger to dump log info; it can not support switch log by size and automatic elimination, 
which will cause leveldb.log growing too large.

```

The format can be described more formally as follows:

```
issue=#xxxx: <what changed>
<BLANK LINE>
<why this change was made>
<BLANK LINE>
```

The first line is the subject and should be no longer than 70 characters, the
second line is always blank, and other lines should be wrapped at 80 characters.
This makes the message to be easier to read on GitHub as well as in various
git tools.
