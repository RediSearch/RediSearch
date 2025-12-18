name: Bug Report
description: File a bug report
title: "[Bug]: "
labels: [bug, triage]
body:
  - type: markdown
    attributes:
      value: |
        Thanks for taking the time to fill out this bug report!

  - type: input
    id: operating_system
    attributes:
      label: OS
      description: Operating system and version (eg Ubuntu 20.04, OSX Catalina)
      placeholder: linux

  - type: architecutre
    id: cpu_arch
    attributes:
      label: CPU Architecture
      description: CPU Architecture (eg: x86, x86-64, arm, etc).
      placeholder: linux

  - type: input
    id: redis_version
    attributes:
      label: Redis Server Version
      description: What version of redis-server are you running (redis-server --version)
      placeholder: latest

  - type: input
    id: redis_module_version
    attributes:
      label: Redis Module Version
      description: What version of this redis module was installed?
      placeholder: latest

  - type: input
    id: was_redis_docker
    attributes:
      label: Docker Installation
      description: What version of this redis module was installed?
      placeholder: latest

  - type: checkboxes
    id: installmethod
    attributes:
      label: Docker installation
      description: Check this box, if you installed the release via a docker.
      options:
        - label: I installed the dockerhub release

  - type: textarea
    id: whathappened
    label: What happened?
    description: Please tell us what happened, and what you expected to happen.
    placeholder: Tell us what you see!

  - type: textarea
    id: logs
    label: Relevant log output
    description: If release, please copy and paste any relevant log output. This will be automatically formatted into code, so no need for backticks.
    render: shell
