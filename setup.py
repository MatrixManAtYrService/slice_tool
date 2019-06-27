from setuptools import setup
setup(name='slicetool',
      version='0.1.0.dev1',
      description='Don\'t pull tables, pull diffs',
      url='https://github.com/kristalinc/slicetool',
      author='Matt Rixman',
      author_email='matt.rixman@clover.com',
      packages=['slicetool'],
      python_requires= '>=3.6',
      install_requires=['sh', 'pymysql', 'SortedContainers'],
      entry_points={'console_scripts' : [

          # sync a billing-slice of meta from source to dest, clobbering dest data
          'pull_bmslice = slicetool.slice:billing_meta',

          # sync billing-ui-slice of meta from source to dest, clobbering dest data
          # (this slice will run server only to the extent necessary to view billing-related pages)
          'pull_buimslice = slicetool.slice:billingUI_meta',

          # sync a billing-slice of billing from source to dest, clobbering dest data
          'pull_bbslice = slicetool.slice:billing_billing',

          # if you add a type of sync to slicetool, you might add an entry for it here

          # given an empty local database, pull a remote schema (with no foreign keys) into it
          'pull_schema = slicetool.schema:pull',

          # remove foreign keys from a local database
          'strip_fk = slicetool.fk:strip',

          # remove unique keys from a local database
          'strip_uk = slicetool.uk:strip',

          # see test/test.sh for more about these
          'pull_test = slicetool.slice:test',

          ]})
