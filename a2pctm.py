import tornado.ioloop

from a2pctm.main import main


if __name__ == "__main__":
    tornado.ioloop.IOLoop.current().run_sync(main)
