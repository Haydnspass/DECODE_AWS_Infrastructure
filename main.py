import sys
import sqs


def setup():
    """Setup AWS infrastructure.
    """
    sqs.setup()
    #TODO

def update_envs():
    """Update environments.
    """
    sqs.update()
    pass

if __name__ == '__main__':
    if '--update_envs' in sys.argv[1:]:
        update_envs()
    else:
        setup()
