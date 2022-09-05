from distutils.core import setup
setup(
  name = 'flypipe',
  packages = ['flypipe'],
  version = '0.0.0',
  license='apache-2.0',
  description = 'Data and Feature store transformations manager',
  author = 'Jose Helio de Brum Muller',
  author_email = 'josemuller.flypipe@gmail.com',
  url = 'https://github.com/flypipe/flypipe.git',
  # download_url = 'https://github.com/user/reponame/archive/v_01.tar.gz',    # I explain this later on
  keywords = ['feature',
              'pipeline',
              'data', 'ml',
              'ds',
              'machine learning',
              'data science',
              'data engineering'],
  install_requires=[
          'networkx==2.8'
      ],
  classifiers=[
    'Development Status :: 3 - Alpha',
    'Intended Audience :: Developers',
    'Development Status :: 1 - Planning',
    'License :: OSI Approved :: Apache Software License',
    'Programming Language :: Python :: 3',
  ],
)
