# Install to a virtual environment

Python virtual environments are a good way to keep this package's commands and dependencies from
cluttering up your system.  If you choose this kind of install, the slicetool commands will not be
available unless you are in the appropriate virtual environment (which you will create).  This
especially useful while developing slicetool itself--but some users may also appreciate the isolation
that venv's provide.

If entering a virtual environment every time you use slicetool sounds like too much work, you might consider a [System Install](system_install.md).

## Requirements

You'll need python 3.6 or higher.  On some systems, you may need to replace `python3` below with `python3.6`, run `python3 --version` to find out.

## The First Time

    # get the repo and enter its root
    ❯  git clone https://github.com/kristalinc/slicetool && cd slicetool

    # create a virtual environment (this makes a folder called .venv)
    ❯ python3 -m venv .venv

    # enter it (by sourcing the script in the newly created folder)
    ❯ source .venv/bin/activate

    # get the latest version of the package manager
    .venv ❯ pip installl --upgrade pip

    # get the latest version of setuptools
    .venv ❯ pip installl --upgrade setuptools

    # add slicetool to the virtual environment (also download dependencies)
    .venv ❯ python setup.py develop

    # view help
    .venv ❯ pull_schema -h
    .venv ❯ strip_fk -h
    .venv ❯ pull_bslice -h

    # do other stuff too

    # exit the venv
    .venv ❯ deactivate

    # slicetool commands aren't avaliable outside of the virtual environment
    ❯ pull_schema -h
        command not found: pull_schema

## Subsequent Times:

    # activate the venv
    ❯ source .venv/bin/activate

    # do stuff
    .venv ❯ pull_bslice --local-password test --local-database meta -ufoo -o some.remote.db -d meta

    # exit the venv
    .venv ❯ deactivate

    ❯ # <-- notice your prompt changed back to normal
