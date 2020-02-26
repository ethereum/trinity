import multiprocessing
import os


# WARNING: Think twice before experimenting with `fork`. The `fork` method does not work well with
# asyncio yet. This might change with Python 3.8 (See https://bugs.python.org/issue22087#msg318140)
MP_CONTEXT = os.environ.get('TRINITY_MP_CONTEXT', 'spawn')


# sets the type of process that multiprocessing will create.
ctx = multiprocessing.get_context(MP_CONTEXT)
