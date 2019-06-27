# System Install

slicetool consists of [several commands](../setup.py) which--once installed, will be available at the command line.
You may want to conser a [venv install](venv_install.md) if:

  - You'd prefer to control when the slicetool commands are available
  - You expect to be making changes to slicetool itself
  - You're willing to tolerate a few extra commands in order to limit interactions with other python apps on your machine
  - You want to use it the way I use it (i.e. the more thoroughly tested way)

If you still want slicetool installed system-wide, then you're in the right place

## Requirements

You'll need python 3.6 or higher

## To install

    # get the repo and enter its root
    ❯  git clone https://github.com/kristalinc/slicetool && cd slicetool

    # invoke the installer
    ❯ python3 setup.py install

## To use

    # view help
    ❯ pull_schema -h
    ❯ strip_fk -h
    ❯ pull_bslice -h

    # do stuff

## Just in case

In some cases you might see a warning like this:

    warning: no previously-included files matching 'yacctab.*' found under directory 'tests'

To fix this, run `pip install -U cffi` and then retry the install.
