import os
from livereload import Server, shell

dirname, filename = os.path.split(os.path.abspath(__file__))

shell('make html',)
server = Server()
server.watch(f'{dirname}/user/*.rst', shell('make html', cwd=dirname))
server.watch(f'{dirname}/*.rst', shell('make html', cwd=dirname))
server.serve(root=f'{dirname}/_build/html')
