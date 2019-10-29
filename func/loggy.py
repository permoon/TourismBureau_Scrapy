import logging

def log_init(f):
    # basic
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M',
                        handlers = [logging.FileHandler(f, 'w', 'utf-8'),])
    # for console log
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # formatter setting
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    console.setFormatter(formatter)
    # addHandler
    logging.getLogger('').addHandler(console)
