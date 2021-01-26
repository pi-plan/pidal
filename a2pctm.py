from tornado.ioloop import IOLoop

from a2pctm.main import main


if __name__ == "__main__":
    IOLoop.current().run_sync(main)
