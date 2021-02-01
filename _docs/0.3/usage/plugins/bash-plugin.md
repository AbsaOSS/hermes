---
layout: docs
title: Plugin - Bash
version: '0.3'
categories:
    - '0.3'
    - 'usage'
    - 'plugins'
redirect_from: /docs/usage/plugins/bash-plugin
---

Bash plugin executes a single command line in the bash using `scala.sys.process`

Arguments (`args`) for Bash plugin are the command you want to run. Arguments array will be joined and delimited by a single white space.

Write arguments (`writeArgs`) are not needed as this plugin does no writing.